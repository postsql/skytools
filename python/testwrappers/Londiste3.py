#!/usr/bin/env python

import os, sys, unittest, psutil, time

from Shell import run_cmd

LONDISTE = 'londiste3'
PGQD = 'pgqd'

pgqd_ini_template = """
[pgqd]

logfile = %(basedir)s/log/pgqd.log
pidfile = %(basedir)s/pid/pgqd.pid

## optional parameters ##

#base_connstr =
#initial_database = template1
#database_list =
#syslog = 0
## optional timeouts ##
#check_period = 60
#retry_period = 30
#maint_period = 120
#ticker_period = 1
"""

class PgQD:
    def __init__(self, basedir='.'):
        self.basedir = basedir
        self.inifile = '%s/pgqd.ini' % self.basedir
        self.logdir = '%s/log/' % self.basedir
        self.piddir = '%s/pid/' % self.basedir
        self.pidfile = self.piddir + 'pgqd.pid'
    def create_ini_file(self):
        "creates .ini file for pgqd and all directories defined in it"
        for subdir in [self.logdir, self.piddir]:
            if not os.path.exists(subdir):
                os.makedirs(subdir)
        pgqd_ini = pgqd_ini_template % self.__dict__
        with open(self.inifile,'w') as f:
            f.write(pgqd_ini)
    def start(self):
        "starts pgqd"
        cmd = [PGQD, '-d', self.inifile]
#        print 'pgqd.start()', cmd
        retcode, out, err = run_cmd(cmd)
    def check(self):
        "checks if pgqd is running"
        pid = int(open(self.pidfile).read())
        try:
            p = psutil.Process(pid)
        except psutil.error.NoSuchProcess:
            raise LookupError, 'PGQD: process with pid %d not running'
        if (PGQD == p.cmdline[0]) and (self.inifile in p.cmdline):
            return True
        return False
    def stop(self):
        "stops pgqd"
        pidfile = '%s/pid/pgqd.pid' % self.basedir
        pid = open(pidfile).read().strip()
        cmd = ['kill', pid]
        retcode, out, err = run_cmd(cmd)

# select table_name, local, merge_state from londiste.get_table_list(E'replika')

londiste3_ini_template = """
[londiste3]
job_name = %(job_name)s
db = %(connect_string)s
queue_name = %(queue_name)s
logfile = %(logfile)s
pidfile = %(pidfile)s
handler_modules = londiste.handlers.merge
"""

# handler_modules = londiste.handlers.merge - this should go into options/args somehere


class Londiste3Node:
    def __init__(self, database, node_type, node_name,
                       queue_name='replika',
                       provider_db=None, job_name=None,
                       basedir = '.'):
        self.database = database
        self.connect_string = self.database.get_connect_string()
        self.node_type = node_type # one of 'root', 'branch', 'leaf'
        self.node_name = node_name
        self.queue_name = queue_name
        self.provider_db = provider_db
        if self.node_type != 'root':
            if not self.provider_db:
                self.get_provider_db()
            self.provider_connect_string = self.provider_db.get_connect_string()
        self.job_name = job_name or 'st3_%s' %  self.database.dbname
        self.basedir = basedir
        self.inifile = '%(basedir)s/%(job_name)s.ini' % self.__dict__
        self.logdir = '%s/log/' % self.basedir
        self.piddir = '%s/pid/' % self.basedir
        self.logfile = '%(basedir)s/log/%(job_name)s.log' % self.__dict__
        self.pidfile = '%(basedir)s/pid/%(job_name)s.pid' % self.__dict__
    def get_provider_db(self):
        "reads provoder db info from node databse"
        raise NotImplemented, 'Londiste3Node.get_provider_db()'
    def create_ini_file(self):
        "creates .ini file for pgqd and all directories defined in it"
        for subdir in [self.logdir, self.piddir]:
            if not os.path.exists(subdir):
                os.makedirs(subdir)
        londiste3_ini = londiste3_ini_template % self.__dict__
        with open(self.inifile,'w') as f:
            f.write(londiste3_ini)
    def wait_replication_state_ok(self, timeout=-1):
        have_waited = 0
        con = self.database. get_db_conn()
        cur = con.cursor()
        repstate_query = """
        select count(*)
          from londiste.get_table_list(%s)
         where merge_state != 'ok'
            or merge_state is null
        """
        while have_waited - timeout:
            cur.execute(repstate_query, (self.queue_name,))
            tables_not_in_sync = cur.fetchall()[0][0]
            if tables_not_in_sync:
                print '\r%3d tables to sync, waited for %4d sec' % (tables_not_in_sync, have_waited),
                sys.stdout.flush()
                have_waited += 1
                if have_waited - timeout == 0:
                    return False, 'Timed out'
                time.sleep(1)
                continue
            else:
                print
                return True
    def create_node(self):
        cmd = [LONDISTE, self.inifile, 'create-'+self.node_type,
               self.node_name] + self.connect_string.split()
        if self.node_type in ['branch', 'leaf']:
            cmd += ['--provider=%s' % self.provider_connect_string]
        retcode, out, err = run_cmd (cmd)
        if retcode:
            raise Exception, 'create failed %s ' % ' '.join(cmd)
    def start(self):
        cmd = [LONDISTE, '-d',  self.inifile, 'replay']
#        print '%s.start()' % self.database.dbname, cmd
        retcode, out, err = run_cmd (cmd)
        if retcode:
            raise Exception, 'start failed %s ' % ' '.join(cmd)
    def check(self):
        "checks if pgqd is running"
        pid = int(open(self.pidfile).read())
        try:
            p = psutil.Process(pid)
        except psutil.error.NoSuchProcess:
            raise LookupError, 'PGQD: process with pid %d not running'
        if (LONDISTE in p.cmdline[1]) and (self.inifile in p.cmdline):
            return True
        return False
    def stop(self):
        "stops the replay process"
        cmd = [LONDISTE, '-d',  self.inifile, '--stop']
        retcode, out, err = run_cmd (cmd)
        if retcode:
            raise Exception, 'start failed: %s.' % ' '.join(cmd)
    def add_tables(self, tablelist=[], flags=['--all']):
        "adds tables to replication, by default adds all user tables"
        cmd = [LONDISTE, self.inifile, 'add-table'] + tablelist + flags
        retcode, out, err = run_cmd(cmd)
        if retcode:
            raise Exception, 'add_tables failed:  %s.' % ' '.join(cmd)
    def status(self):
        "prints replication status"
        cmd = [LONDISTE, self.inifile, 'status']
        retcode, stdout, stderr = run_cmd(cmd)
        if retcode:
            print stderr
            raise Exception, 'status failed:  %s.' % ' '.join(cmd)
        print stdout
    def compare_tables(self):
        "compares tables between provider and subscriber"
        cmd = [LONDISTE, self.inifile, 'compare']
        retcode, stdout, stderr = run_cmd(cmd)
        if retcode:
            print stderr
            raise Exception, 'compare failed:  %s.' % ' '.join(cmd)
        print stdout, stderr
    def execute(self, sql, name=None):
        if not name:
            name = '/tmp/%04d%02d%02dt%02d%02d%02d.ddl' % time.localtime(time.time())[:6]
        with open(name, 'w') as f:
            f.write(sql)
        cmd = [LONDISTE, self.inifile, 'execute', name]
        retcode, stdout, stderr = run_cmd(cmd)
        if retcode:
            print stderr
            raise Exception, 'execute failed:  %s.' % ' '.join(cmd) 
        print stdout

throttled_pgbench_script = """
\set nbranches :scale
\set ntellers 10 * :scale
\set naccounts 100000 * :scale
\setrandom aid 1 :naccounts
\setrandom bid 1 :nbranches
\setrandom tid 1 :ntellers
\setrandom delta -5000 5000
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
\setrandom wait 10 100
\sleep :wait ms
"""

from PGDatabase import PGDatabase
from PGBench import PGBench

class Londiste3Cluster:
    def __init__(self, root_db_name, branch_db_name, leaf_db_name=None, basedir = 'st3test'):
        self.root_db_name = root_db_name
        self.branch_db_name = branch_db_name
        self.leaf_db_name = leaf_db_name
        self.basedir = basedir
        self.pgqd = PgQD(basedir = self.basedir)
        self.db1 = PGDatabase(self.root_db_name)
        self.node1 = Londiste3Node(self.db1, 'root', 'node1', basedir = self.basedir)
        self.pgb = PGBench(2,self.db1)
        self.db2 = PGDatabase(self.branch_db_name)
        self.node2 = Londiste3Node(self.db2, 'branch', 'node2', provider_db=self.db1, basedir = self.basedir)
        if self.leaf_db_name:
            self.init_leaf_node()
    def init_leaf_node(self):
        self.db3 = PGDatabase(self.leaf_db_name )
        self.node3 = Londiste3Node(self.db3, 'leaf', 'node3', provider_db=self.db1, basedir = self.basedir)
    def start(self):
        self.setup_root_schema()
        self.setup_replicated_schema(self.db2)
        if self.leaf_db_name:
            self.setup_replicated_schema(self.db3)
        self.setup_node(self.node1)
        self.setup_node(self.node2)
        if self.leaf_db_name:
            self.setup_node(self.node3)
        self.pgqd.create_ini_file()
        self.pgqd.start()
    def setup_root_schema(self):
        # create database
        self.db1.createdb()
        # create schemas, populate with data and modify for replication
        self.pgb.pgb_init_db()
        self.pgb.modify_db_for_replication()
    def setup_replicated_schema(self, db):
        # create database
        db.createdb()
        # copy schema from root database
        db.copy_schema_from(self.db1)
    def setup_node(self, node):
        # install londiste3 support and start replay process
        node.create_ini_file()
        node.create_node()
        node.start()
    def check_processes(self):
        time.sleep(1) # let things settle
        print 'pgqd.check()', self.pgqd.check()
        print 'l3_node1.check()', self.node1.check()
        print 'l3_node2.check()', self.node2.check()
        # print replication tree
        self.node1.status()
    def add_tables(self, tablelist=['--all'], wait=True):
        self.node1.add_tables(tablelist)
        self.node2.add_tables(tablelist)
        if wait:
            print 'waiting for subscription to finish'
            time.sleep(3) # let things settle
            self.node2.wait_replication_state_ok()
    def start_pgbench_in_background(self, seconds=120):
        # starts a long pgbench run in background to run for 2 minutes
        with open('/tmp/throttled.pgbench', 'w') as f:
            f.write(throttled_pgbench_script)
        self.pgb.pgb_run(seconds=seconds, concurrency=5, filename='/tmp/throttled.pgbench', background=True)
    def check_data(self):
        self.node2.compare_tables()
        if self.leaf_db_name:
            self.node3.compare_tables()
    def tearDown(self):
        self.pgqd.stop()
        self.node1.stop()
        self.node2.stop()
        if self.leaf_db_name:
            self.node3.stop()
        self.db1.dropdb()
        self.db2.dropdb()
        if self.leaf_db_name:
            self.db3.dropdb()

if __name__ == '__main__':
    "test setup of londiste3 replica"
    import PGDatabase, PGBench
    db1 = PGDatabase.PGDatabase('l3db1')
    pgb = PGBench.PGBench(2,db1)
    l3_node1 = Londiste3Node(db1, 'root', 'node1', basedir = 'st3test')
    db2 = PGDatabase.PGDatabase('l3db2')
    l3_node2 = Londiste3Node(db2, 'branch', 'node2', provider_db=db1, basedir = 'st3test')
    db3 = PGDatabase.PGDatabase('l3db3')
    l3_node3 = Londiste3Node(db3, 'leaf', 'node3', provider_db=db2, basedir = 'st3test')
    pgqd = PgQD(basedir = 'st3test')
    if 0:
        pgqd.create_ini_file()
        # create db1 and populate using pgbench -i
        db1.createdb()
        pgb.pgb_init_db()
        pgb.modify_db_for_replication()
        # install londiste3 on db1/node1
        l3_node1.create_ini_file()
        l3_node1.create_node()
        l3_node1.start()
        # create other dbs
        db2.createdb()
        l3_node2.create_ini_file()
        l3_node2.create_node()
        l3_node2.start()
        # 
        db3.createdb()
        l3_node3.create_ini_file()
        l3_node3.create_node()
        l3_node3.start()
        # start ticker daemon
        pgqd.start()
    if 1:
        time.sleep(1)
        print 'pgqd.check()', pgqd.check()
        print 'l3_node1.check()', l3_node1.check()
        print 'l3_node2.check()', l3_node2.check()
        print 'l3_node3.check()', l3_node3.check()
    if 0:
        db2.copy_schema_from(db1)
        db3.copy_schema_from(db2)
    if 0:
        l3_node1.add_tables()
        l3_node2.add_tables()
        l3_node3.add_tables()
    pgb.pgb_run(10,10)
    print 'waiting for node2 to sync'
    l3_node2.wait_replication_state_ok()
    l3_node2.compare_tables()
    print 'waiting for node3 to sync'
    l3_node3.wait_replication_state_ok()
    l3_node3.compare_tables()
    print 'replication graph and lags'
    l3_node1.status()
