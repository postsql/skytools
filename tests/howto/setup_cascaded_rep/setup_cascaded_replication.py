#!/usr/bin/env python

import sys, time

sys.path.append('../../../python/testwrappers')

from Londiste3 import Londiste3Cluster


def setup_londiste_cascaded_replication():
    l3c = Londiste3Cluster('l3db1', 'l3db2')
    l3c.start()
    l3c.leaf_db_name  = 'l3db3'
    l3c.init_leaf_node()
    l3c.setup_replicated_schema(l3c.db3)
    l3c.db3.cleanup_londiste_provider()
    l3c.setup_node(l3c.node3)
    

def cleanup():
    l3c = Londiste3Cluster('l3db1', 'l3db2', 'l3db3')
    l3c.tearDown()

if __name__ == '__main__':
    if len(sys.argv) <= 1 or sys.argv[1] == 'a':
        setup_londiste_and_add_col_then_add_new_leaf()
        print 'use: psql l3db3 -c "\d pgbench_tellers" to check for column pgbench_tellers.testadd in added database' 
    elif len(sys.argv) > 1 and sys.argv[1] == 'X':
        print 'cleaning up!'
        cleanup()


