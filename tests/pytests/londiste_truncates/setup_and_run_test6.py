#!/usr/bin/env python

import sys, time

sys.path.append('../..')

from st3wrapper import Londiste3

def test6():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.start()
    l3c.add_tables()
    l3c.start_pgbench_in_background(30)
    time.sleep(5)
    l3c.db1.execute('truncate pgbench_history')
    time.sleep(5)
    l3c.db1.execute('truncate pgbench_history_p1')
    time.sleep(5)
    l3c.check_data()

def cleanup():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.tearDown()

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'X':
        print 'cleaning up!'
        cleanup()
    else:
        test6()


