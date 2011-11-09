#!/usr/bin/python

import sys

sys.path.append('../../st3wrapper')

from Londiste2 import Londiste2
from PGDatabase import PGDatabase
from PGBench import PGBench

def setup_londiste2_replication():
    db = PGDatabase('pgbtest', port='5432')
    db2 = PGDatabase('pgbtest2', port='5432')
    londiste = Londiste2(db, db2)
    db.createdb()
    pgbench = PGBench(2, db=db)
    pgbench.preparedb()
    db2.createdb()
    db2.copy_schema_from(db)
    londiste.create_ticker_ini()
    londiste.install_ticker()
    londiste.start_ticker()
    londiste.create_provider_ini()
    londiste.create_subscriber_ini()
    londiste.install_londiste_on_provider()
    londiste.install_londiste_on_subscriber()
    londiste.start_londiste_replay()
    londiste.add_tables_on_provider(['--all'])
    londiste.add_tables_on_subscriber(['--all'])
    londiste.wait_for_subscription()

if __name__ == '__main__':
    setup_londiste2_replication()





