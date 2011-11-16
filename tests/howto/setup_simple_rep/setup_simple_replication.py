#!/usr/bin/env python

import sys, time

sys.path.append('../../../python/testwrappers')

from Londiste3 import Londiste3Cluster


def setup_londiste_simple_replication():
    print '= Setting up simple Londiste3 replication =\n'
    print 'Hannu Krosing\n'
    l3c = Londiste3Cluster('l3db1', 'l3db2', howto=True)
    l3c.start()
    print '== the setup of simple 2 node cluster is done ==\n'
    print '''You can use the pgbench command to generate more load and
    londiste --compare to check if replicaton is ok.
    '''
    

def cleanup():
    l3c = Londiste3Cluster('l3db1', 'l3db2')
    l3c.tearDown()

if __name__ == '__main__':
    if len(sys.argv) <= 1 or sys.argv[1] == 'a':
        setup_londiste_simple_replication()
    elif len(sys.argv) > 1 and sys.argv[1] == 'X':
        print 'cleaning up!'
        cleanup()


