#! /usr/bin/env python

"""PgQ ticker and maintenance.


Config template::

    [pgqadm]

    # should be globally unique
    job_name = pgqadm_somedb

    db = dbname=provider port=6000 host=127.0.0.1

    # how often to run maintenance [minutes]
    maint_delay_min = 5

    # how often to check for activity [secs]
    loop_delay = 0.5

    logfile = ~/log/%(job_name)s.log
    pidfile = ~/pid/%(job_name)s.pid

    use_skylog = 0

"""

import sys

import pkgloader
pkgloader.require('skytools', '3.0')

import skytools, pgq
from pgq.cascade.admin import CascadeAdmin

"""TODO:
pgqadm ini check
"""

command_usage = """
%prog [options] INI CMD [subcmd args]

local queue commands:
  ticker                   start ticking & maintenance process

  status                   show overview of queue health

  install                  install code into db
  create QNAME             create queue
  drop QNAME               drop queue
  register QNAME CONS      install code into db
  unregister QNAME CONS    install code into db
  config QNAME [VAR=VAL]   show or change queue config

cascaded queue commands:
  create-node
  rename-node
  pause-node
  resume-node
  change-provider
  tag-alive
  tag-dead
  switchover
  failover
"""

config_allowed_list = {
    'queue_ticker_max_count': 'int',
    'queue_ticker_max_lag': 'interval',
    'queue_ticker_idle_period': 'interval',
    'queue_rotation_period': 'interval',
}

class PGQAdmin(skytools.DBScript):
    __doc__ = __doc__
    def __init__(self, args):
        """Initialize pgqadm."""
        skytools.DBScript.__init__(self, 'pgqadm', args)
        self.set_single_loop(1)


        if len(self.args) < 2:
            print("need command")
            sys.exit(1)

        int_cmds = {
            'create': self.create_queue,
            'drop': self.drop_queue,
            'register': self.register,
            'unregister': self.unregister,
            'install': self.installer,
            'config': self.change_config,
        }

        cascade_cmds = ['create-node']

        cmd = self.args[1]
        if cmd == "ticker":
            script = pgq.SmallTicker(args)
        elif cmd == "status":
            script = pgq.PGQStatus(args)
        elif cmd in cascade_cmds:
            script = CascadeAdmin(self.service_name, 'db', args)
        elif cmd in int_cmds:
            script = None
            self.work = int_cmds[cmd]
        else:
            print("unknown command")
            sys.exit(1)

        if self.pidfile:
            self.pidfile += ".admin"
        self.run_script = script

    def start(self):
        if self.run_script:
            self.run_script.start()
        else:
            skytools.DBScript.start(self)

    def reload(self):
        skytools.DBScript.reload(self)
        self.set_single_loop(1)

    def init_optparse(self, parser=None):
        p = skytools.DBScript.init_optparse(self, parser)
        p.set_usage(command_usage.strip())
        p.add_option("--queue", help = 'cascading: specify queue name')
        return p

    def installer(self):
        objs = [
            skytools.DBLanguage("plpgsql"),
            skytools.DBFunction("txid_current_snapshot", 0, sql_file="txid.sql"),
            skytools.DBSchema("pgq", sql_file="pgq.sql"),
        ]

        db = self.get_database('db')
        curs = db.cursor()
        skytools.db_install(curs, objs, self.log)
        db.commit()

    def create_queue(self):
        qname = self.args[2]
        self.log.info('Creating queue: %s' % qname)
        self.exec_sql("select pgq.create_queue(%s)", [qname])

    def drop_queue(self):
        qname = self.args[2]
        self.log.info('Dropping queue: %s' % qname)
        self.exec_sql("select pgq.drop_queue(%s)", [qname])

    def register(self):
        qname = self.args[2]
        cons = self.args[3]
        self.log.info('Registering consumer %s on queue %s' % (cons, qname))
        self.exec_sql("select pgq.register_consumer(%s, %s)", [qname, cons])

    def unregister(self):
        qname = self.args[2]
        cons = self.args[3]
        self.log.info('Unregistering consumer %s from queue %s' % (cons, qname))
        self.exec_sql("select pgq.unregister_consumer(%s, %s)", [qname, cons])

    def change_config(self):
        if len(self.args) < 3:
            qlist = self.get_queue_list()
            for qname in qlist:
                self.show_config(qname)
            return

        qname = self.args[2]
        if len(self.args) == 3:
            self.show_config(qname)
            return

        alist = []
        for el in self.args[3:]:
            k, v = el.split('=')
            if k not in config_allowed_list:
                qk = "queue_" + k
                if qk not in config_allowed_list:
                    raise Exception('unknown config var: '+k)
                k = qk
            expr = "%s=%s" % (k, skytools.quote_literal(v))
            alist.append(expr)
        self.log.info('Change queue %s config to: %s' % (qname, ", ".join(alist)))
        sql = "update pgq.queue set %s where queue_name = %s" % (
                        ", ".join(alist), skytools.quote_literal(qname))
        self.exec_sql(sql, [])

    def exec_sql(self, q, args):
        self.log.debug(q)
        db = self.get_database('db')
        curs = db.cursor()
        curs.execute(q, args)
        db.commit()

    def show_config(self, qname):
        fields = []
        for f, kind in config_allowed_list.items():
            if kind == 'interval':
                sql = "extract('epoch' from %s)::text as %s" % (f, f)
                fields.append(sql)
            else:
                fields.append(f)
        klist = ", ".join(fields)
        q = "select " + klist + " from pgq.queue where queue_name = %s"

        db = self.get_database('db')
        curs = db.cursor()
        curs.execute(q, [qname])
        res = curs.dictfetchone()
        db.commit()

        if res is None:
            print("no such queue: " + qname)
            return

        print(qname)
        for k in config_allowed_list:
            n = k
            if k[:6] == "queue_":
                n = k[6:]
            print("    %s\t=%7s" % (n, res[k]))

    def get_queue_list(self):
        db = self.get_database('db')
        curs = db.cursor()
        curs.execute("select queue_name from pgq.queue order by 1")
        rows = curs.fetchall()
        db.commit()
        
        qlist = []
        for r in rows:
            qlist.append(r[0])
        return qlist

if __name__ == '__main__':
    script = PGQAdmin(sys.argv[1:])
    script.start()



