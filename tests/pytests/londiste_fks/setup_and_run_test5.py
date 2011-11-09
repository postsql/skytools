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

add_replicated_fk = """
    ALTER TABLE pgbench_tellers ADD CONSTRAINT pgbench_tellers_branches_fk FOREIGN KEY(bid) REFERENCES pgbench_branches;
"""

add_fk_from_replicated_to_non_replicated = """
    ALTER TABLE pgbench_history ADD CONSTRAINT pgbench_history_accounts_fk FOREIGN KEY(aid) REFERENCES pgbench_accounts;
"""

def test5a():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.start()
    l3c.db1.execute(drop_all_fks)
    l3c.db2.execute(drop_all_fks)
    l3c.add_tables(['pgbench_branches', 'pgbench_tellers', 'pgbench_history'])
    l3c.node1.execute(add_replicated_fk)

def test5b():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.node1.execute(add_fk_from_replicated_to_non_replicated)

def test5c():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.add_tables(['pgbench_accounts'])

def cleanup():
    l3c = Londiste3.Londiste3Cluster('l3db1', 'l3db2')
    l3c.tearDown()

if __name__ == '__main__':
    if sys.argv[1] == 'a':
        test5a()
        print 'check that psql l3db2 -c "\d pgbench_branches" has foreign key pgbench_tellers_branches_fk'
    elif sys.argv[1] == 'b':
        test5b()
        print 'check that psql l3db1 -c "\d pgbench_accounts" has foreign key pgbench_history_accounts_fk'
        print 'check that psql l3db2 -c "\d pgbench_accounts" has NOT foreign key pgbench_history_accounts_fk'
        print 'it should be in psql l3db2 -c "select * from londiste.pending_fkeys"';
        print '\nNB! it seems that the fix is missing in current londiste3 !'
    elif sys.argv[1] == 'c':
        test5b()
        print 'This currently fails, as fk is stored wrong'
        print 'NB! it seems that the fix is missing in current londiste3 !'
    elif sys.argv[1] == 'X':
        print 'cleaning up!'
        cleanup()


