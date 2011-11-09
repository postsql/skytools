#!/usr/bin/env python

from subprocess import Popen, PIPE, call

def run_cmd(cmd, shell=False):
    print '# cmd:\n%s' % (' '.join(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE, shell=shell)
    out, err = p.communicate()
#    if p.returncode:
#        print 'Command FAILED (err:%d): %s\n%s\n%s' % (p.returncode, ' '.join(cmd), out, err)
    return (p.returncode,out, err)

def run_pipe(cmd1, cmd2):
    print '#pipe:\n %s | %s' % (' '.join(cmd1), ' '.join(cmd2))
    p1 = Popen(cmd1, stdout=PIPE)
    p2 = Popen(cmd2, stdin=p1.stdout, stdout=PIPE)
    out, err = p2.communicate()
#    if p2.returncode:
#        print 'Pipe FAILED (err:%d): %s\n%s\n%s' % (p.returncode, ' '.join(cmd1 + ['|'] + cmd2), out, err)
    return (p2.returncode, out, err)
    
from threading import Thread

running_threads = []

def run_in_background(cmd):
    print '#starting command in background: \n%s' % (' '.join(cmd))
    t = Thread(target=run_cmd, args=[cmd])
    t.start()
    running_threads.append(t)
    return t

