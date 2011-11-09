#!/usr/bin/python

import unittest
import psycopg2

PGBENCH = "/usr/lib/postgresql/8.4/bin/pgbench"

from Shell import run_cmd, run_in_background

from PGDatabase import PGDatabase

pgbencdb_mods = """
    -- modifies pgbench database for londiste replication testing
    -- add primary key to history table
    ALTER TABLE pgbench_history ADD COLUMN hid SERIAL PRIMARY KEY;
    -- add foreign keys
    ALTER TABLE pgbench_tellers ADD CONSTRAINT pgbench_tellers_branches_fk FOREIGN KEY(bid) REFERENCES pgbench_branches;
    ALTER TABLE pgbench_accounts ADD CONSTRAINT pgbench_accounts_branches_fk FOREIGN KEY(bid) REFERENCES pgbench_branches;
    ALTER TABLE pgbench_history ADD CONSTRAINT pgbench_history_branches_fk FOREIGN KEY(bid) REFERENCES pgbench_branches;
    ALTER TABLE pgbench_history ADD CONSTRAINT pgbench_history_tellers_fk FOREIGN KEY(tid) REFERENCES pgbench_tellers;
    ALTER TABLE pgbench_history ADD CONSTRAINT pgbench_history_accounts_fk FOREIGN KEY(aid) REFERENCES pgbench_accounts;
    -- add columns for moddate triggers
    ALTER TABLE pgbench_branches ADD COLUMN moddate timestamp;
    ALTER TABLE pgbench_accounts ADD COLUMN moddate timestamp;
    ALTER TABLE pgbench_tellers ADD COLUMN moddate timestamp;
    -- add moddate triggers
    CREATE TRIGGER pgbench_branches_moddatetime BEFORE UPDATE ON pgbench_branches FOR EACH ROW EXECUTE PROCEDURE moddatetime (moddate);
    CREATE TRIGGER pgbench_accounts_moddatetime BEFORE UPDATE ON pgbench_accounts FOR EACH ROW EXECUTE PROCEDURE moddatetime (moddate);
    CREATE TRIGGER pgbench_tellers_moddatetime BEFORE UPDATE ON pgbench_tellers FOR EACH ROW EXECUTE PROCEDURE moddatetime (moddate);
    -- rule based split of history table 
    CREATE TABLE pgbench_history_p0 (CHECK (tid <= 5)) INHERITS (pgbench_history);
    ALTER TABLE pgbench_history_p0 ADD CONSTRAINT pgbench_history_p0_pk PRIMARY KEY (hid);
    CREATE RULE pgbench_history_p0 AS ON INSERT TO pgbench_history WHERE (tid <= 5) DO INSTEAD INSERT INTO pgbench_history_p0 VALUES (NEW.*);
    CREATE TABLE pgbench_history_p1 (CHECK (tid >5 and tid <= 10)) INHERITS (pgbench_history);
    ALTER TABLE pgbench_history_p1 ADD CONSTRAINT pgbench_history_p1_pk PRIMARY KEY (hid);
    CREATE RULE pgbench_history_p1 AS ON INSERT TO pgbench_history WHERE (tid >5 and tid <= 10) DO INSTEAD INSERT INTO pgbench_history_p1 VALUES (NEW.*);
    CREATE TABLE pgbench_history_p2 (CHECK (tid >10 and tid <= 15)) INHERITS (pgbench_history);
    ALTER TABLE pgbench_history_p2 ADD CONSTRAINT pgbench_history_p2_pk PRIMARY KEY (hid);
    CREATE RULE pgbench_history_p2 AS ON INSERT TO pgbench_history WHERE (tid >10 and tid <= 15) DO INSTEAD INSERT INTO pgbench_history_p2 VALUES (NEW.*);
    CREATE TABLE pgbench_history_p3 (CHECK (tid >15 and tid <= 20)) INHERITS (pgbench_history);
    ALTER TABLE pgbench_history_p3 ADD CONSTRAINT pgbench_history_p3_pk PRIMARY KEY (hid);
    CREATE RULE pgbench_history_p3 AS ON INSERT TO pgbench_history WHERE (tid >15 and tid <= 20) DO INSTEAD INSERT INTO pgbench_history_p3 VALUES (NEW.*);
"""

class PGBench:
    "class for interacting with pgbench utility"
    def __init__(self, scale, db=None, dbname=None, host=None, port=None, user=None, passwd=None):
        self.scale = scale
        if db:
            self.database = db
        else:
            self.database = PGDatabase(dbname, host, port, user, passwd)
    def pgb_init_db(self):
        "just run 'pgbench -i -s'"
        # run pgbench -i -s %(self.scale)s
        cmd = [PGBENCH, '-i', '-s', str(self.scale), '-F', '80'] + self.database.get_db_cmdline_args()
        run_cmd(cmd)
    def modify_db_for_replication(self):
        "add pk's, foreign keys, partitions, ..."
        # add support for moddatetime triggers from contrib
        cmd = ['psql'] + self.database.get_db_cmdline_args() + ['-f', '/usr/share/postgresql/8.4/contrib/moddatetime.sql']
#        print cmd
        run_cmd(cmd)
        # modify db and commit
        with open('/tmp/prepare_pgbenchdb_for_londiste.sql', 'w') as f:
            f.write(pgbencdb_mods)
        cmd = ['psql'] + self.database.get_db_cmdline_args() + ['-f', '/tmp/prepare_pgbenchdb_for_londiste.sql']
        run_cmd(cmd)
        # run pgbench -T 10 to populate db with some more data
        # self.pgb_run(10,16)
    def pgb_run (self, seconds=60, concurrency=25, filename=None, background=False):
        # run pgbench -T 10 to populate db with some data
        cmd = [PGBENCH, '-T', '%d' % seconds, '-c', '%d' % concurrency] + self.database.get_db_cmdline_args()
        if filename:
            cmd += ['-f', filename]
        if background:
            run_in_background(cmd)
        else:
            run_cmd(cmd)
    def preparedb(self):
        "prepare db for replication"
        self.pgb_init_db()
        self.modify_db_for_replication()
    def capture_unmodified_schema(self):
        "read and save result of 'pg_dump -s'"

class PGBenchTest(unittest.TestCase):
    "tests that pgbench is runnable"
    def setUp(self):
        self.pgb = PGBench(2, 'pgbtest', port='5432')
    def test_createdb(self):
        self.pgb.database.createdb()
    def test_dropdb(self):
        self.pgb.database.dropdb()
    def test_get_connect_string(self):
        self.assertEqual(self.pgb.database.get_connect_string(), 'dbname=pgbtest port=5432')
    def test_get_db_cmdline_args(self):
        self.assertEqual(self.pgb.database.get_db_cmdline_args(), ['pgbtest', '-p', '5432'])
    def test_su_connect(self):
        sucon = self.pgb.database.get_su_conn()
    def test_connect(self):
        con = psycopg2.connect(self.pgb.database.get_connect_string())
    def test_pgb_init_db(self):
        self.pgb.pgb_init_db()
    def test_modify_db_for_replication(self):
        self.pgb.modify_db_for_replication()
        

def suite():
    suite = unittest.TestSuite()
    suite.addTest(PGBenchTest('test_get_connect_string'))
    suite.addTest(PGBenchTest('test_get_db_cmdline_args'))
    suite.addTest(PGBenchTest('test_su_connect'))
    suite.addTest(PGBenchTest('test_createdb'))
    suite.addTest(PGBenchTest('test_connect'))
    suite.addTest(PGBenchTest('test_pgb_init_db'))
    suite.addTest(PGBenchTest('test_modify_db_for_replication'))
#    suite.addTest(PGBenchTest('test_dropdb'))
    return suite

if __name__ == '__main__':
#    unittest.main()
    unittest.TextTestRunner(verbosity=9).run(suite())




