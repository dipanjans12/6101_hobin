#! /usr/bin/python

import multiprocessing
import subprocess
import sys
import time


def Indent(s0, ind):
  s1 = ""
  tokens = s0.split("\n")
  for i in range(len(tokens)):
    if i != 0:
      s1 += "\n"
    for j in range(ind):
      s1 += " "
    s1 += tokens[i]
  return s1


def KillJavaProcess(name):
  cmd = "jps -l | grep %s" % name
  #print cmd
  out = None
  try:
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
  except subprocess.CalledProcessError as e:
    #print "  No process with name %s" % name
    return

  if out != None and len(out) > 0:
    lines = out.split("\n")
    pids = []
    for l in lines:
      t = l.split()
      if len(t) == 2:
        pids.append(t[0])

    #sys.stdout.write("Killing [%s] " % pids)
    #sys.stdout.flush()
    cmd = "kill %s" % (" ".join(pids))
    #print cmd
    subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)

    cmd = "jps -l | grep %s" % name
    while True:
      out = None
      try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
      except subprocess.CalledProcessError as e:
        #print " killed"
        break
      #sys.stdout.write(".")
      #sys.stdout.flush()


def KillSchedWorker():
  sys.stdout.write("Killing scheduler and worker ... ")
  sys.stdout.flush()
  bt = time.time()
  jobs = []
  jobs.append(multiprocessing.Process(target=KillJavaProcess, args=("scheduler.jar",)))
  jobs.append(multiprocessing.Process(target=KillJavaProcess, args=("worker.jar",)))
  for j in jobs:
    j.start()
  for j in jobs:
    j.join()
  et = time.time()
  print " %.3f sec" % (et - bt)
  print ""


def RunSubpSafe(cmd, q):
  cmd += " || true"
  try:
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    #print "out: [%s]" % out
    q.put(out)
  except subprocess.CalledProcessError as e:
    print "subprocess.CalledProcessError: %s" % e
    pass


def RunSubp(cmd, q):
  out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
  q.put(out)


_sched = None
_workers = []
_sched_q = multiprocessing.Queue()
_worker_qs = []


def RunSchedWorker(num_w):
  global _sched, _workers
  print "Running scheduler ..."
  _sched = multiprocessing.Process(target=RunSubpSafe, args=("java -jar scheduler.jar 51000", _sched_q,))
  _sched.start()
  time.sleep(0.5)
  print "Running %d worker(s) ..." % num_w
  for i in range(num_w):
    q = multiprocessing.Queue()
    _worker_qs.append(q)
    _workers.append(multiprocessing.Process(target=RunSubpSafe,
      args=("java -jar worker.jar localhost 51000 %d" % (51001 + i), q,)))
  for j in _workers:
    j.start()
  time.sleep(0.5)
  print ""


def KillJoinSchedWorker():
  KillSchedWorker()

  print "scheduler"
  print Indent(_sched_q.get(), 2)

  for i in range(len(_worker_qs)):
    print "worker %d" % i
    print Indent(_worker_qs[i].get(), 2)

  _sched.join()
  for w in _workers:
    w.join()


def RunJobs():
  print "jobs.Hello"
  print "----------"
  print subprocess.check_output("java -jar client.jar localhost 51000 jobs.jar jobs.Hello",
      stderr=subprocess.STDOUT, shell=True)

  print "jobs.Mvm"
  print "--------"
  print subprocess.check_output("java -jar client.jar localhost 51000 jobs.jar jobs.Mvm",
      stderr=subprocess.STDOUT, shell=True)


def TestRunHello(runs):
  sys.stdout.write("Running Hello %d times ... " % runs)
  sys.stdout.flush()
  jobs = []
  qs = []
  for i in range(runs):
    q = multiprocessing.Queue()
    qs.append(q)
    jobs.append(multiprocessing.Process(target=RunSubp, args=("java -jar client.jar localhost 51000 jobs.jar jobs.Hello", q,)))

  bt = time.time()
  for j in jobs:
    j.start()

  out = ""
  for q in qs:
    out += q.get()
  for j in jobs:
    j.join()

  et = time.time()
  print "%f sec" % (et - bt)
  print Indent(out, 2)


def main(argv):
  KillSchedWorker()

  RunSchedWorker(2)

  #RunJobs()
  TestRunHello(2)

  KillJoinSchedWorker()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
