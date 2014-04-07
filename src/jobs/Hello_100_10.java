package jobs;

import java.text.SimpleDateFormat;
import common.*;

//Hello World
public class Hello_100_10 extends Job {
  static final SimpleDateFormat _sdf = new SimpleDateFormat("HH:mm:ss.SSS");

  @Override
  public void config() {
    setNumTasks(100); //set the number of tasks
  }

  @Override
  public void task(int tId) {
    try{
      //System.out.println(_sdf.format(System.currentTimeMillis()) +
      //    " task"+tId+": Hello_100_10"); //this string will be printed out from worker instead of client
      Thread.sleep(10);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
