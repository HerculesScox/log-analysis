package parse;

import conf.LAConf;
import base.Job;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.PatternParse;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by zhangyun on 4/28/15.
 */
public class TaskLogParser {
  private static Log LOG = LogFactory.getLog(JHParser.class);
  private LogInput logInput;
  private LAConf conf;

  public TaskLogParser(LAConf conf) {
    this.conf = conf;
    logInput = new LogInput(conf);
  }

  public HashMap<String,ArrayList<Path>> parsePath() throws IOException{
    // build a map , from job id to log files path of all task
    HashMap<String,ArrayList<Path>> idToPaths = new HashMap<String,ArrayList<Path>>();
    HashSet<Path> paths = logInput.genTFPath();
    for(Path p : paths){

      StringBuilder builder = new StringBuilder("job");
      builder.append( PatternParse.jobID(p.toString()));
      String jobID = builder.toString();

      if( ! idToPaths.containsKey(jobID)){
        ArrayList<Path> taskPaths = new ArrayList<Path>();
        taskPaths.add(p);
        idToPaths.put(jobID , taskPaths);
      }else{
        idToPaths.get(jobID).add(p);
      }
    }
    return idToPaths;
  }

  public void parse(Job job ,Path taskLog) throws  IOException{
    FileSystem fs = taskLog.getFileSystem(conf);
    FSDataInputStream in = fs.open(taskLog);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String line;
    while( (line = br.readLine()) != null){
      System.out.println(line);
    }
  }
}
