package jobs;

import java.text.SimpleDateFormat;
import common.*;

//Hello World
public class Hello_5_100 extends Job {
  static final SimpleDateFormat _sdf = new SimpleDateFormat("HH:mm:ss.SSS");

  @Override
  public void config() {
    setNumTasks(5); //set the number of tasks
  }

  @Override
  public void task(int tId) {
    try{
      //System.out.println(_sdf.format(System.currentTimeMillis()) +
      //    " task"+tId+": Hello_10_100"); //this string will be printed out from worker instead of client
      Thread.sleep(100);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
