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
    pid = out.split()[0]
    #sys.stdout.write("Killing [%s] " % pid)
    #sys.stdout.flush()
    cmd = "kill %s" % pid
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
  print "Killing scheduler and worker ..."
  jobs = []
  jobs.append(multiprocessing.Process(target=KillJavaProcess, args=("scheduler.jar",)))
  jobs.append(multiprocessing.Process(target=KillJavaProcess, args=("worker.jar",)))
  for j in jobs:
    j.start()
  for j in jobs:
    j.join()
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
_worker = None
_sched_q = multiprocessing.Queue()
_worker_q = multiprocessing.Queue()


def RunSchedWorker():
  global _sched, _worker
  print "Running scheduler ..."
  _sched = multiprocessing.Process(target=RunSubpSafe, args=("java -jar scheduler.jar 51000", _sched_q,))
  _sched.start()
  time.sleep(0.5)
  print "Running worker ..."
  _worker = multiprocessing.Process(target=RunSubpSafe, args=("java -jar worker.jar localhost 51000 51001", _worker_q,))
  #_worker = multiprocessing.Process(target=RunWorker, args=(_worker_q,))
  _worker.start()
  time.sleep(0.5)
  print ""


def KillJoinSchedWorker():
  KillSchedWorker()

  print "scheduler"
  print Indent(_sched_q.get(), 2)

  print "worker"
  print Indent(_worker_q.get(), 2)

  _sched.join()
  _worker.join()


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

  RunSchedWorker()

  #RunJobs()
  TestRunHello(2)

  KillJoinSchedWorker()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
