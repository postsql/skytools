#!/usr/bin/python

import unittest
import psycopg2

#from itertools import chain

def flatten(list_of_lists):
    # found this on the Interweb, can't understand how it works
    return [item for l in list_of_lists for item in l]

PGBENCH = "/usr/lib/postgresql/8.4/bin/pgbench"

from Shell import  run_cmd, run_pipe

class PGDatabase:
    "class for manipulating databases"
    def __init__(self, dbname, host=None, port=None, user=None, passwd=None):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db_con = None
        self.su_con = None
    def get_db_cmdline_args(self, cdict=None):
        cdict = cdict or self.__dict__
        cmdp = {'host':'h', 'port':'p', 'user':'u'}
        return [self.dbname] + flatten([('-'+cmdp[key],cdict[key]) for key in cmdp if cdict[key]])
    def get_connect_string(self, cdict=None):
        cdict = cdict or self.__dict__
        connp = ['dbname','host','port','user']
        return ' '.join(['%s=%s' % (key,cdict[key]) for key in connp if cdict[key] ])
    def get_db_conn(self):
        if not self.db_con:
            self.db_con = psycopg2.connect(self.get_connect_string())
        return self.db_con
    def get_su_conn(self):
        if not self.su_con:
            su_dict = self.__dict__.copy()
            su_dict['dbname'] = 'postgres'
            self.su_con = psycopg2.connect(self.get_connect_string(su_dict))
            self.su_con.set_isolation_level(0)
        return self.su_con
    def dropdb(self):
        con = self.get_su_conn()
        cur = con.cursor()
        print 'sql:', 'DROP DATABASE %s;' % self.dbname
        cur.execute('DROP DATABASE %s;' % self.dbname)
    def createdb(self):
        con = self.get_su_conn()
        cur = con.cursor()
        print 'sql:', 'CREATE DATABASE %s;' % self.dbname
        cur.execute('CREATE DATABASE %s;' % self.dbname)
    def copy_schema_from(self, copyfrom_db):
        connargs_src = copyfrom_db.get_db_cmdline_args()
        connargs_dst = self.get_db_cmdline_args()
        cmd1 = ['pg_dump', '-s'] + connargs_src
        cmd2 = ['psql'] + connargs_dst
        run_pipe(cmd1, cmd2)
    def cleanup_londiste_provider(self):
        con = psycopg2.connect(self.get_connect_string())
        cur = con.cursor()
        cur.execute("select * from pg_namespace n WHERE(n.nspname !~ '^pg_temp_' )")
        for r in cur.fetchall():
            print r
        cur.execute('DROP SCHEMA londiste CASCADE')
        cur.execute('DROP SCHEMA pgq CASCADE')
        cur.execute('DROP SCHEMA pgq_ext CASCADE')
        cur.execute('DROP SCHEMA pgq_node CASCADE')
        
    def execute(self, sql):
        con = psycopg2.connect(self.get_connect_string())
        cur = con.cursor()
        cur.execute(sql)
        try:
            res = cur.fetchall()
        except psycopg2.ProgrammingError:
            res = None
        con.commit()
        cur.close()
        con.close()

class PGDatabaseTest(unittest.TestCase):
    "tests that pgbench is runnable"
    def setUp(self):
        self.db = PGDatabase('pgbtest', port='5432')
    def test_createdb(self):
        self.db.createdb()
    def test_dropdb(self):
        self.db.dropdb()
    def test_get_connect_string(self):
        self.assertEqual(self.db.get_connect_string(), 'dbname=pgbtest port=5432')
    def test_get_db_cmdline_args(self):
        self.assertEqual(self.db.get_db_cmdline_args(), ['pgbtest', '-p', '5432'])
    def test_su_connect(self):
        sucon = self.db.get_su_conn()
    def test_connect(self):
        con = psycopg2.connect(self.db.get_connect_string())

def suite():
    suite = unittest.TestSuite()
    suite.addTest(PGDatabaseTest('test_get_connect_string'))
    suite.addTest(PGDatabaseTest('test_get_db_cmdline_args'))
    suite.addTest(PGDatabaseTest('test_su_connect'))
    suite.addTest(PGDatabaseTest('test_createdb'))
    suite.addTest(PGDatabaseTest('test_connect'))
    suite.addTest(PGDatabaseTest('test_dropdb'))
    return suite

if __name__ == '__main__':
#    unittest.main()
    unittest.TextTestRunner(verbosity=9).run(suite())





