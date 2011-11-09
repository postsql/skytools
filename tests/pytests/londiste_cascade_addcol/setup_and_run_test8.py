#!/usr/bin/env python

import sys, time

sys.path.append('../..')

from st3wrapper import Londiste3


def test8():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.start()
    l3c.node1.execute('ALTER TABLE pgbench_tellers ADD COLUMN testadd text', 'addcol.ddl')
    time.sleep(3)
    l3c.leaf_db_name  = 'l3db3'
    l3c.init_leaf_node()
    l3c.setup_replicated_schema(l3c.db3)
    l3c.db3.cleanup_londiste_provider()
    l3c.setup_node(l3c.node3)
    

def cleanup():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2', 'l3db3')
    l3c.tearDown()

if __name__ == '__main__':
    if len(sys.argv) <= 1 or sys.argv[1] == 'a':
        test8()
        print 'use: l3db3 -c "\d pgbench_tellers" to check for column pgbench_tellers.testadd in added database' 
    elif len(sys.argv) > 1 and sys.argv[1] == 'X':
        print 'cleaning up!'
        cleanup()


