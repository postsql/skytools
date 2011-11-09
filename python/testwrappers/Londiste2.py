#!/usr/bin/python

"""
 * install skytools 2
 * set up londiste replication
 * manage ticker conf
 * manage ticker process(es)
 * manage londiste conf
 * manage londiste replay process(es)
 * check replication status ( londiste ... subscriber tables)
 * wait for 'all ok' status
 * check replication lag and lastseen (execute("select ...") on master)
 * wait for "lag < N"
 * check replication correctness ( londiste ... compare )

where "manage P process" means

 * start P when neccessary
 * check P is running
 * end P when neccessary
 * kill P if can't end nicely
"""

import unittest


import os

PGQADM = 'pgqadm.py'
LONDISTE = 'londiste.py'

from Shell import run_cmd

# skytools is installed by asking user to run 
# sudo dpkg -i skytools-* 2.1 debs

ticker_ini_template = """
[pgqadm]
job_name = %(provider_dbname)s_ticker
db = %(provider_connect)s
 
# how often to run maintenance [seconds]
maint_delay = 600
 
# how often to check for activity [seconds]
loop_delay = 0.1
logfile = nodes/%(provider_dbname)s/log/%%(job_name)s.log
pidfile = nodes/%(provider_dbname)s/pid/%%(job_name)s.pid
"""

londiste_ini_template = """
[londiste]
job_name = %(provider_dbname)s_to_%(subscriber_dbname)s

provider_db = %(provider_connect)s
subscriber_db = %(subscriber_connect)s

# it will be used as sql ident so no dots/spaces
pgq_queue_name = %(provider_dbname)s_repq

logfile = nodes/%(conf_dir)s/log/%%(job_name)s.log
pidfile = nodes/%(conf_dir)s/pid/%%(job_name)s.pid
"""

class Londiste2:
    def __init__(self, masterdb, slavedb=None):
        self.masterdb = masterdb
        self.slavedb = slavedb
        self.ticker_ini = 'nodes/%s/ticker.ini' % masterdb.dbname
        self.provider_ini = 'nodes/%s/%s_to_None.ini' % (masterdb.dbname, masterdb.dbname)
        self.subscriber_ini = 'nodes/%s/%s_to_%s.ini' % (slavedb.dbname, masterdb.dbname, slavedb.dbname)
        self.pgq_queue_name = '%s_repq' % masterdb.dbname
    def create_directories(self):
        "creates node directory and log and pid directories defined .ini files"
        for nodename in [self.slavedb.dbname, self.masterdb.dbname]:
            for subdir in ['log', 'pid']:
                path = "nodes/%s/%s/" % (nodename, subdir)
                if not os.path.exists(path):
                    os.makedirs(path)
    def create_ticker_ini(self):
        "creates ticker.ini file nodes/<masterdb.dbname>/ticker.ini"
        provider_dbname = self.masterdb.dbname
        provider_connect = self.masterdb.get_connect_string()
        ticker_ini = ticker_ini_template % locals()
        self.create_directories() # in case they are not there yet
        f = open(self.ticker_ini, 'w')
        f.write(ticker_ini)
        f.close()
    def install_ticker(self):
        "installs pgq on master"
        run_cmd([PGQADM, self.ticker_ini, 'install'])
    def start_ticker(self):
        "starts ticker on provider"
        # print "start_ticker() ::",[PGQADM, self.ticker_ini, 'ticker', '-d']
        run_cmd([PGQADM, self.ticker_ini, 'ticker', '-d'])
    def check_ticker(self):
        "checks if ticker is running"
        dbname = self.masterdb.dbname
        ticker_pid_file = 'nodes/%s/pid/%s_ticker.pid' % (dbname, dbname)
        ticker_pid = int(open(ticker_pid_file).read())
        import psutil
        p = psutil.Process(ticker_pid)
        return 'pgqadm.py' in p.cmdline[1]  # check if running script is pgqadm.py
    def stop_ticker(self):
        "stops ticker on provider"
        run_cmd([PGQADM, self.ticker_ini, '--stop'])
        
    def create_provider_ini(self):
        "creates provider-only londiste conf (for installing londiste)"
        provider_dbname = self.masterdb.dbname
        subscriber_dbname = None
        provider_connect = self.masterdb.get_connect_string()
        subscriber_connect = None
        conf_dir = provider_dbname
        londiste_ini = londiste_ini_template % locals()
        f = open(self.provider_ini, 'w')
        f.write(londiste_ini)
        f.close()
    def create_subscriber_ini(self):
        "creates londiste conf for replication"
        provider_dbname = self.masterdb.dbname
        subscriber_dbname = self.slavedb.dbname
        provider_connect = self.masterdb.get_connect_string()
        subscriber_connect = self.slavedb.get_connect_string()
        conf_dir = subscriber_dbname
        londiste_ini = londiste_ini_template % locals()
#        print 'CREATING:', londiste_ini
        f = open(self.subscriber_ini, 'w')
        f.write(londiste_ini)
        f.close()
    def install_londiste_on_provider(self):
        run_cmd([LONDISTE, self.provider_ini, 'provider', 'install'])
    def install_londiste_on_subscriber(self):
        run_cmd([LONDISTE, self.subscriber_ini, 'subscriber', 'install'])
    def start_londiste_replay(self):
        run_cmd([LONDISTE, self.subscriber_ini, 'replay', '-d'])
    def check_londiste_replay(self):
        "checks if londiste replay is on"
        
        
    def stop_londiste_replay(self):
        run_cmd([LONDISTE, self.subscriber_ini, '--stop'])
    def add_tables_on_provider(self, tablelist):
#        print [LONDISTE, self.provider_ini, 'provider', 'add'] + tablelist
        run_cmd([LONDISTE, self.provider_ini, 'provider', 'add'] + tablelist)
    def add_tables_on_subscriber(self, tablelist):
        run_cmd([LONDISTE, self.subscriber_ini, 'subscriber', 'add'] + tablelist)
    def wait_for_subscription(self, timeout=60): # timeout in sec
        pass 
    def check_lag(self):
        pass
        # run query "SELECT queue_name, consumer_name, lag, last_seen FROM pgq.get_consumer_info()";
    def check_data(self):
        run_cmd([LONDISTE, self.provider_ini, 'compare'])

class Londiste2Test(unittest.TestCase):
    "tests that pgbench is runnable"
    def setUp(self):
        from PGDatabase import PGDatabase
        self.db = PGDatabase('pgbtest', port='5432')
        self.db2 = PGDatabase('pgbtest2', port='5432')
        self.londiste = Londiste2(self.db, self.db2)
    def tearDown(self):
        pass
    def test_replica_setup(self):
        from PGBench import PGBench
        self.db.createdb()
        self.pgbench = PGBench(2, db=self.db)
        self.pgbench.preparedb()
        self.db2.createdb()
        self.db2.copy_schema_from(self.db)
        if 0: # if londiste was installed on source
            self.db2.cleanup_londiste_provider()
    def test_create_ticker_ini(self):
        self.londiste.create_ticker_ini()
    def test_install_ticker(self):
        self.londiste.install_ticker()
    def test_start_ticker(self):
        self.londiste.start_ticker()
    def test_create_provider_ini(self):
        self.londiste.create_provider_ini()
    def test_create_subscriber_ini(self):
        self.londiste.create_subscriber_ini()
    def test_install_londiste_on_provider(self):
        self.londiste.install_londiste_on_provider()
    def test_install_londiste_on_subscriber(self):
        self.londiste.install_londiste_on_subscriber()
    def test_start_londiste_replay(self):
        self.londiste.start_londiste_replay()
    def test_add_tables_on_provider(self):
        self.londiste.add_tables_on_provider(['--all'])
    def test_add_tables_on_subscriber(self):
        self.londiste.add_tables_on_subscriber(['--all'])
    def test_wait_for_subscription(self):
        self.londiste.wait_for_subscription()
    def test_check_lag(self):
        self.londiste.check_lag()
    def test_check_data(self):
        self.londiste.check_data()
    def test_(self):
        self.londiste.x()
    def test_(self):
        self.londiste.x()

def suite():
    suite = unittest.TestSuite()
    suite.addTest(Londiste2Test('test_replica_setup'))
#    return suite
    suite.addTest(Londiste2Test('test_create_ticker_ini'))
    suite.addTest(Londiste2Test('test_install_ticker'))
    suite.addTest(Londiste2Test('test_start_ticker'))
    suite.addTest(Londiste2Test('test_create_provider_ini'))
    suite.addTest(Londiste2Test('test_install_londiste_on_provider'))
    suite.addTest(Londiste2Test('test_create_subscriber_ini'))
    suite.addTest(Londiste2Test('test_install_londiste_on_subscriber'))
    suite.addTest(Londiste2Test('test_start_londiste_replay'))
    suite.addTest(Londiste2Test('test_add_tables_on_provider'))
    suite.addTest(Londiste2Test('test_add_tables_on_subscriber'))
    suite.addTest(Londiste2Test('test_wait_for_subscription'))
    suite.addTest(Londiste2Test('test_check_lag'))
    suite.addTest(Londiste2Test('test_check_data'))
#    suite.addTest(Londiste2Test(''))
    return suite

if __name__ == '__main__':
#    unittest.main()
    unittest.TextTestRunner(verbosity=9).run(suite())



