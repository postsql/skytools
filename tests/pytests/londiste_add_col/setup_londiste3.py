#!/usr/bin/env python

import sys, time

sys.path.append('../../st3wrapper')

from Londiste3 import PgQD, Londiste3Node
from PGDatabase import PGDatabase
from PGBench import PGBench

def setup_londiste3_replication():
    db1 = PGDatabase('l3db1')
    pgb = PGBench(2,db1)
    l3_node1 = Londiste3Node(db1, 'root', 'node1', basedir = 'st3test')
    db2 = PGDatabase('l3db2')
    l3_node2 = Londiste3Node(db2, 'branch', 'node2', provider_db=db1, basedir = 'st3test')
    pgqd = PgQD(basedir = 'st3test')
    if 0:
	    pgqd.create_ini_file()
	    # create and populate db
	    db1.createdb()
	    pgb.pgb_init_db()
	    pgb.modify_db_for_replication()
	    # create node
	    l3_node1.create_ini_file()
	    l3_node1.create_node()
	    l3_node1.start()
	    # create other dbs and nodes
	    db2.createdb()
	    l3_node2.create_ini_file()
	    l3_node2.create_node()
	    l3_node2.start()
	    # start ticker daemon
	    pgqd.start()
	    # wait a sec
	    time.sleep(1)
	    # check aall daemons are running
	    print 'pgqd.check()', pgqd.check()
	    print 'l3_node1.check()', l3_node1.check()
	    print 'l3_node2.check()', l3_node2.check()
	    # copy schema
	    db2.copy_schema_from(db1)
	    # add tables
	    l3_node1.add_tables()
	    l3_node2.add_tables()
	    pgb.pgb_run(10,10)
	    print 'waiting for node2 to sync'
	    l3_node2.wait_replication_state_ok()
	    l3_node2.compare_tables()
	    print 'replication graph and lags'
    l3_node1.status()
    print 'Adding column on provider'
    l3_node1.execute('alter table pgbench_accounts add column reptestcolumn text')
    # wait a few sec
    time.sleep(3)
    print 'now see if pgbench_accounts.reptestcolumn exists in l3db2\n'
    print "use:\n"
    print "   psql l3db2 -c '\d pgbench_accounts'"

if __name__ == '__main__':
    setup_londiste3_replication()
