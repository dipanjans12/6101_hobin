package scheduler;

import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import common.*;

public class Scheduler {
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
            System.out.println("Worker "+n.id+" "+n.addr+" "+n.port+" created");
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
          // get the next job (with a fair share policy, RR first)
          //System.out.printf("_Scheduler:run: jobs.take() ...\n");
          Job j = jobs.take();
          //System.out.printf("_Scheduler:run: job: %d %d %d\n", j.id, j.nextTask.get(), j.numTasks);
          if (j.nextTask.get() != j.numTasks) {
            jobs.put(j);
            j.RunNextTask(w, cluster);
            break;
          }
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
      System.out.printf("%s AddJob j=%d num_tasks=%d className=%s\n",
          _sdf.format(System.currentTimeMillis()), jobId, numTasks, className);
      jobs.put(j);

      // wait for all the tasks of the job to finish
      while (j.num_remaining_tasks.get() != 0) {
        synchronized (j.num_remaining_tasks) {
          //System.out.printf("_Scheduler.Run: j.num_remaining_tasks.wait();\n");
          j.num_remaining_tasks.wait();
        }
      }
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
    AtomicInteger nextTask;
    AtomicBoolean task_start_notified;
    AtomicInteger num_remaining_tasks;

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
      this.nextTask = new AtomicInteger(0);
      this.dis = dis;
      this.dos = dos;

      task_start_notified = new AtomicBoolean();
      num_remaining_tasks = new AtomicInteger(numTasks);
    }

    // round-robin tasks within a job
    void RunNextTask(WorkerNode w, Cluster cluster) {
      class _RunTask implements Runnable {
        WorkerNode w;
        int job_id;
        int task_id;
        String className;
        DataInputStream dis;
        DataOutputStream dos;
        Cluster cluster;
        AtomicBoolean task_start_notified;
        AtomicInteger num_remaining_tasks;

        _RunTask(
            WorkerNode w,
            int job_id,
            int task_id,
            String className,
            DataInputStream dis,
            DataOutputStream dos,
            Cluster cluster,
            AtomicBoolean task_start_notified,
            AtomicInteger num_remaining_tasks) {
          this.w = w;
          this.job_id = job_id;
          this.task_id = task_id;
          this.className = className;
          this.dis = dis;
          this.dos = dos;
          this.cluster = cluster;
          this.task_start_notified = task_start_notified;
          this.num_remaining_tasks = num_remaining_tasks;
            }

        public void run() {
          try {
            final int numTasksPerWorker = 1;

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
            while(wis.readInt() == Opcode.task_finish) {
              synchronized(dos) {
                dos.writeInt(Opcode.job_print);
                //dos.writeUTF("task "+wis.readInt()+" finished on worker "+w.id);
                dos.writeUTF(_Scheduler._sdf.format(System.currentTimeMillis()) + " task "+wis.readInt()+" finished on worker "+w.id);
                dos.flush();
              }
            }

            //disconnect and free the worker
            wis.close();
            wos.close();
            workerSocket.close();
            cluster.addFreeWorkerNode(w);

            // notify when all tasks of the job finish
            int nrt = num_remaining_tasks.decrementAndGet();
            if (nrt == 0) {
              synchronized (num_remaining_tasks) {
                num_remaining_tasks.notifyAll();
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }

      int task_id = nextTask.getAndIncrement();
      System.out.printf("%s w=%d j=%d t=%d\n",
          _sdf.format(System.currentTimeMillis()), w.id, id, task_id);
      Thread t = new Thread(new _RunTask(w, id, task_id, className, dis, dos, cluster, task_start_notified, num_remaining_tasks));
      t.start();
    }
  }
}
