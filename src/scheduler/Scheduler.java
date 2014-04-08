package scheduler;

import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import common.*;

public class Scheduler {
  public static final SimpleDateFormat _sdf = new SimpleDateFormat("HH:mm:ss.SSS");

  private class ProcessWorkerregNewjob implements Runnable {
    Socket _s;

    ProcessWorkerregNewjob(Socket s) {
      _s = s;
    }

    public void run() {
      try {
        DataInputStream dis = new DataInputStream(_s.getInputStream());
        DataOutputStream dos = new DataOutputStream(_s.getOutputStream());
        int code = dis.readInt();

        //a connection from worker reporting itself
        if(code == Opcode.new_worker){
          //include the worker into the cluster
          WorkerNode n = cluster.createWorkerNode( dis.readUTF(), dis.readInt());
          if( n == null){
            dos.writeInt(Opcode.error);
          }
          else{
            dos.writeInt(Opcode.success);
            dos.writeInt(n.id);
            System.out.printf("%s worker_start: w=%d %s:%d\n",
                _sdf.format(System.currentTimeMillis()), n.id, n.addr, n.port);
          }
          dos.flush();
        } else if (code == Opcode.new_job) {
          //a connection from client submitting a job

          String className = dis.readUTF();
          long len = dis.readLong();

          //send out the jobId
          int jobId = jobIdNext.getAndIncrement();
          dos.writeInt(jobId);
          dos.flush();

          //receive the job file and store it to the shared filesystem
          String fileName = new String("fs/."+jobId+".jar");
          FileOutputStream fos = new FileOutputStream(fileName);
          int count;
          byte[] buf = new byte[65536];
          while(len > 0) {
            count = dis.read(buf);
            if(count > 0){
              fos.write(buf, 0, count);
              len -= count;
            }
          }
          fos.flush();
          fos.close();
          
          int numTasks = JobFactory.getJob(fileName, className).getNumTasks();

          _Scheduler.Run(jobId, numTasks, className, dis, dos);

          //notify the client
          dos.writeInt(Opcode.job_finish);
          dos.flush();
        }

        dis.close();
        dos.close();
        _s.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  int schedulerPort;
  Cluster cluster;
  AtomicInteger jobIdNext = new AtomicInteger();

  Scheduler(int p) {
    schedulerPort = p;
    cluster = new Cluster();
    jobIdNext.getAndIncrement();
  }

  public static void main(String[] args) {
    Scheduler scheduler = new Scheduler(Integer.parseInt(args[0]));
    scheduler.run();
  }

  public void run() {
    try{
      //create a ServerSocket listening at specified port
      ServerSocket serverSocket = new ServerSocket(schedulerPort);
      Thread _sched = new Thread(new _Scheduler(cluster));
      _sched.start();

      while(true){
        //accept connection from worker or client
        Socket socket = serverSocket.accept();
        Thread t = new Thread(new ProcessWorkerregNewjob(socket));
        t.start();
      }
    } catch(Exception e) {
      e.printStackTrace();
    }
      
    //serverSocket.close();
  }
}


//the data structure for a cluster of worker nodes
class Cluster {
  // This is only for tracking worker node ids. Better change it so that the
  // worker node ids are not reused.
  ArrayList<WorkerNode> workers; //all the workers

  LinkedList<WorkerNode> freeWorkers; //the free workers
  
  Cluster() {
    workers = new ArrayList<WorkerNode>();
    freeWorkers = new LinkedList<WorkerNode>();
  }

  WorkerNode createWorkerNode(String addr, int port) {
    WorkerNode n = null;

    synchronized(workers) {
      n = new WorkerNode(workers.size(), addr, port);
      workers.add(n);
    }
    addFreeWorkerNode(n);

    return n;
  }

  // get a free worker node
  WorkerNode getFreeWorkerNode() {
    WorkerNode n = null;

    try{
      synchronized (freeWorkers) {
        while (freeWorkers.size() == 0) {
          freeWorkers.wait();
        }
        n = freeWorkers.remove();
      }
      n.status = 2;
    } catch(Exception e) {
      e.printStackTrace();
    }

    return n;
  }

  // put a free worker node
  void addFreeWorkerNode(WorkerNode n) {
    n.status = 1;
    synchronized(freeWorkers) {
      freeWorkers.add(n);
      freeWorkers.notifyAll();
    }
  }
}


//the data structure of a worker node
class WorkerNode {
  int id;
  String addr;
  int port;
  int status; //WorkerNode status: 0-sleep, 1-free, 2-busy, 4-failed

  WorkerNode(int i, String a, int p) {
    id = i;
    addr = a;
    port = p;
    status = 0;
  }
}


class _Scheduler implements Runnable {
  private static BlockingQueue<Job> jobs = new LinkedBlockingQueue<Job>();
  public static final SimpleDateFormat _sdf = new SimpleDateFormat("HH:mm:ss.SSS");

  private Cluster cluster;

  _Scheduler(Cluster cluster) {
    this.cluster = cluster;
  }

  public void run() {
    try {
      while (true) {
        //System.out.printf("_Scheduler:run: getFreeWorkerNode() ...\n");
        // wait and get a free worker
        WorkerNode w = cluster.getFreeWorkerNode();

        while (true) {
          // Round-robin next job. TODO: implement fair scheduling
          Job j = jobs.take();
          if (j.Completed())
            continue;
          jobs.put(j);
          if (j.RunNextTask(w, cluster))
            break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static public void Run(
      int jobId,
      int numTasks,
      String className,
      DataInputStream dis,
      DataOutputStream dos) {
    try {
      Job j = new Job(jobId, numTasks, className, dis, dos);
      System.out.printf("%s job_add: j=%d num_tasks=%d className=%s\n",
          _sdf.format(System.currentTimeMillis()), jobId, numTasks, className);
      jobs.put(j);
      j.WaitForCompletion();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static class Job {
    final int id;
    final int numTasks;
    final String className;
    DataInputStream dis;
    DataOutputStream dos;
    Queue<Integer> tasks_to_run;
    Set<Integer> tasks_completed;
    AtomicBoolean task_start_notified;

    Job(int id,
        int numTasks,
        String className,
        DataInputStream dis,
        DataOutputStream dos) {
      this.id = id;
      this.numTasks = numTasks;
      if (this.numTasks <= 0)
        throw new RuntimeException(String.format("Unexpected numTasks %d", numTasks));
      this.className = className;

      tasks_to_run = new ConcurrentLinkedQueue<Integer>();
      for (int i = 0; i < numTasks; i ++)
        tasks_to_run.add(i);

      tasks_completed = new HashSet<Integer>();

      this.dis = dis;
      this.dos = dos;

      task_start_notified = new AtomicBoolean();
    }

    boolean Completed() {
      int tc = 0;
      synchronized (tasks_completed) {
        tc = tasks_completed.size();
      }
      return (tc == numTasks);
    }

    void WaitForCompletion()
      throws InterruptedException {
      // wait for all the tasks of the job to finish
      //System.out.printf("_Scheduler: tasks_completed.wait();\n");
      synchronized (tasks_completed) {
        tasks_completed.wait();
      }
    }

    // round-robin tasks within a job
    boolean RunNextTask(WorkerNode w, Cluster cluster) {
      class _RunTask implements Runnable {
        WorkerNode w;
        int job_id;
        int task_id;
        String className;
        DataInputStream dis;
        DataOutputStream dos;
        Cluster cluster;
        AtomicBoolean task_start_notified;

        _RunTask(
            WorkerNode w,
            int job_id,
            int task_id,
            String className,
            DataInputStream dis,
            DataOutputStream dos,
            Cluster cluster,
            AtomicBoolean task_start_notified) {
          this.w = w;
          this.job_id = job_id;
          this.task_id = task_id;
          this.className = className;
          this.dis = dis;
          this.dos = dos;
          this.cluster = cluster;
          this.task_start_notified = task_start_notified;
        }

        public void run() {
          try {
            {
              final int numTasksPerWorker = 1;

              System.out.printf("%s task_start: w=%d j=%d t=%d\n",
                  _sdf.format(System.currentTimeMillis()), w.id, id, task_id);

              boolean ts_notified = task_start_notified.getAndSet(true);
              if (! ts_notified) {
                //notify the client
                synchronized(dos) {
                  dos.writeInt(Opcode.job_start);
                  dos.flush();
                }
              }

              //assign the tasks to the worker
              Socket workerSocket = new Socket(w.addr, w.port);
              DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
              DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());

              wos.writeInt(Opcode.new_tasks);
              wos.writeInt(job_id);
              wos.writeUTF(className);
              wos.writeInt(task_id);
              wos.writeInt(numTasksPerWorker);
              wos.flush();

              //repeatedly process the worker's feedback
              while (true) {
                int w_op = wis.readInt();
                if (w_op == Opcode.task_finish) {
                  synchronized(dos) {
                    dos.writeInt(Opcode.job_print);
                    //dos.writeUTF("task "+wis.readInt()+" finished on worker "+w.id);
                    dos.writeUTF(_Scheduler._sdf.format(System.currentTimeMillis()) + " task "+wis.readInt()+" finished on worker "+w.id);
                    dos.flush();
                  }
                } else if (w_op == Opcode.worker_finish) {
                  break;
                } else if (w_op == Opcode.worker_heartbeat) {
                  //wd.Reset();
                  System.out.printf("%s heartbeat from worker %d\n", _sdf.format(System.currentTimeMillis()), w.id);
                } else {
                  throw new RuntimeException(String.format("Unexpected worker opcode %d", w_op));
                }
              }

              //disconnect and free the worker
              wis.close();
              wos.close();
              workerSocket.close();
              cluster.addFreeWorkerNode(w);

              System.out.printf("%s task_finish: w=%d j=%d t=%d\n",
                  _sdf.format(System.currentTimeMillis()), w.id, id, task_id);
            }

            synchronized (tasks_completed) {
              tasks_completed.add(task_id);

              // notify when all tasks of the job finish
              if (tasks_completed.size() == numTasks)
                tasks_completed.notifyAll();
            }
          } catch (EOFException e) {
            // handle worker failure. put back the task.
            System.out.printf("%s task_fail: w=%d j=%d t=%d\n",
                _sdf.format(System.currentTimeMillis()), w.id, id, task_id);
            tasks_to_run.add(task_id);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }

      Integer task_id = tasks_to_run.poll();
      if (task_id == null)
        return false;

      Thread t = new Thread(new _RunTask(w, id, task_id, className, dis, dos, cluster, task_start_notified));
      t.start();
      return true;
    }
  }
}
