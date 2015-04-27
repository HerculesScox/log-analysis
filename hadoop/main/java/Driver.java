import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parse.JhistFileParser;
import parse.LogInput;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by zhangyun on 4/24/15.
 */

public class Driver {
  private static Log LOG = LogFactory.getLog(Driver.class);
  private LogInput logInput;
  private LAConf conf;

  public Driver(LAConf conf) {
    this.conf = conf;
    logInput = new LogInput(conf);
  }

  public void  ParseJsonFile( ) throws IOException {
    HashSet<Path> jpaths = logInput.genJFPath();
    for (Path path : jpaths){
      System.out.println(path);
      FileSystem fs = path.getFileSystem(conf);
      JhistFileParser jp = new JhistFileParser( fs, path);
      jp.parse().printAll();
    }
  }

  public static void main(String[] args ){
    LAConf conf = new LAConf();
    try{
      Driver pad = new Driver(conf);

      pad.ParseJsonFile();
      /*JobHistoryParser jp = new JobHistoryParser( fs,path);

      jp.parse().printAll();
      for( Map.Entry<TaskID, JobHistoryParser.TaskInfo> task : jp.parse().getAllTasks().entrySet()){
        System.out.println("task = " + task.getKey() +"\n value = ");
        task.getValue().printAll();
      }*/
      System.out.println(" ============================================ ");



    }catch (IOException e){
      e.printStackTrace();
    }
  }
}
