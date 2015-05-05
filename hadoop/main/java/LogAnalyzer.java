import base.Job;
import base.Query;
import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.TaskID;
import parse.JHParser;
import parse.JhistFileParser;
import parse.TaskLogParser;

import java.io.IOException;
import java.util.*;

/**
 * Created by zhangyun on 4/24/15.
 */
public class LogAnalyzer extends Observable{
  private static Log LOG = LogFactory.getLog(LogAnalyzer.class);

  private LAConf conf;
  private static HashSet<Query> queries;


  public LogAnalyzer(LAConf conf) {
    this.conf = conf;
    this.queries = new HashSet<Query>();
  }

  /**
   * When All information of one query was collected, QueryLog class
   * should notify Register(Observer) to sore them into DB.
   */

  public void analyze() throws IOException {
    JHParser pad = new JHParser(conf);
    TaskLogParser taskParser = new TaskLogParser(conf);
    HashMap<String,ArrayList<Path>> jobIDToTaskPath = taskParser.parsePath();
    if(jobIDToTaskPath == null){
      System.out.println("Not found!");
    }
    pad.ParseJsonFile();
    Collection<Query> queries = pad.getQueries().values();
    for(Query q : queries){
      System.out.println("================================================");
      System.out.println(">>QUERY "+ q.getQueryString());
      System.out.println("\t job dependency : " );
      for(Job job : q.getJobs()){
        String jobID = job.getJobInfo().getJobid().toString();

        for(Map.Entry<TaskID, JhistFileParser.TaskInfo> map : job.getJobInfo().getTasksMap().entrySet()){
          System.out.println("----------------- ");
          System.out.println(map.getKey().toString() + " => ");
          map.getValue().printAll();
        }

        for(Path p : jobIDToTaskPath.get(jobID)){
         // taskParser.parse(job, p);
          System.out.println("\tTask path : "+ p.toString());
        }
      }
      System.out.println("================================================");
      break;
    }

  }

  public void gatherSucc(){
    setChanged();
    notifyObservers();
  }
}
