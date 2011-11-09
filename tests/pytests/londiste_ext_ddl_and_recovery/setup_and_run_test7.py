#!/usr/bin/env python

import sys, time

sys.path.append('../..')

from st3wrapper import Londiste3

drop_all_fks = """
    ALTER TABLE pgbench_tellers DROP CONSTRAINT pgbench_tellers_branches_fk ;
    ALTER TABLE pgbench_accounts DROP CONSTRAINT pgbench_accounts_branches_fk;
    ALTER TABLE pgbench_history DROP CONSTRAINT pgbench_history_branches_fk;
    ALTER TABLE pgbench_history DROP CONSTRAINT pgbench_history_tellers_fk;
    ALTER TABLE pgbench_history DROP CONSTRAINT pgbench_history_accounts_fk;
"""

def test7a():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.start()
    l3c.node1.execute(drop_all_fks, 'dropfk.ddl')
    time.sleep(10)
#    l3c.start_pgbench_in_background(30)
    l3c.db2.execute('drop table pgbench_history cascade')
    # this succeeds on both provider and subscriber
    time.sleep(10)
    l3c.node1.execute('ALTER TABLE pgbench_tellers ADD CONSTRAINT pgbench_tellers_branches_fk FOREIGN KEY(bid) REFERENCES pgbench_branches', 'ok.ddl')
    # this succeeds on provider but fails on subscriber
    time.sleep(10)
    l3c.node1.execute('ALTER TABLE pgbench_history ADD CONSTRAINT pgbench_history_accounts_fk FOREIGN KEY(aid) REFERENCES pgbench_accounts;', 'fail.ddl')



def test7b():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.db2.execute("select londiste.execute_start('replika','fail.ddl','-- replacement ddl',False);")
    l3c.db2.execute("select londiste.execute_finish('replika','fail.ddl');")

def cleanup():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.tearDown()

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print 'needs argument a, b or X'
        sys.exit()
    if sys.argv[1] == 'a':
        test7a()
        print 'use "tail -f st3test/log/st3_l3db2.log" to see 2nd command failing'
    elif  sys.argv[1] == 'b':
        test7b()
        print 'use "tail -f st3test/log/st3_l3db2.log" to see replication continue'
    elif len(sys.argv) > 1 and sys.argv[1] == 'X':
        print 'cleaning up!'
        cleanup()


