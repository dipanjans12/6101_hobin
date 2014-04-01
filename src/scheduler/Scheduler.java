package scheduler;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import common.*;

public class Scheduler {

  private class ProcessReq implements Runnable {
    Socket _s;

    ProcessReq(Socket s) {
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
          

          //get the tasks
          int taskIdStart = 0;
          int numTasks = JobFactory.getJob(fileName, className).getNumTasks();

          //get a free worker
          WorkerNode n = cluster.getFreeWorkerNode();

          //notify the client
          dos.writeInt(Opcode.job_start);
          dos.flush();

          //assign the tasks to the worker
          Socket workerSocket = new Socket(n.addr, n.port);
          DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
          DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());

          wos.writeInt(Opcode.new_tasks);
          wos.writeInt(jobId);
          wos.writeUTF(className);
          wos.writeInt(taskIdStart);
          wos.writeInt(numTasks);
          wos.flush();

          //repeatedly process the worker's feedback
          while(wis.readInt() == Opcode.task_finish) {
            dos.writeInt(Opcode.job_print);
            dos.writeUTF("task "+wis.readInt()+" finished on worker "+n.id);
            dos.flush();
          }

          //disconnect and free the worker
          wis.close();
          wos.close();
          workerSocket.close();
          cluster.addFreeWorkerNode(n);

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

      while(true){
        //accept connection from worker or client
        Socket socket = serverSocket.accept();
        Thread t = new Thread(new ProcessReq(socket));
        t.start();
      }
    } catch(Exception e) {
      e.printStackTrace();
    }
      
    //serverSocket.close();
  }
  

  //the data structure for a cluster of worker nodes
  class Cluster {
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

    // Hobin: get a free worker node
    WorkerNode getFreeWorkerNode() {
      WorkerNode n = null;

      try{
        synchronized(freeWorkers) {
          while(freeWorkers.size() == 0) {
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

    // Hobin: put a free worker node
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


}
