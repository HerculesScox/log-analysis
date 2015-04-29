import base.Job;
import base.Query;
import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import parse.JHParser;
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
      System.out.println(">>QUERY "+ q.getQueryString());
      System.out.println("\t job dependency : " );
      for(String k :  q.getWorkflowAdjacencies().keySet()){
        System.out.println("\t\t "+k +" => "+q.getWorkflowAdjacencies().get(k));
      }
      for(Job job : q.getJobs()){
        String jobID = job.getJobInfo().getJobid().toString();
        System.out.println("\tJOBID "+ jobID);
        for(Path p : jobIDToTaskPath.get(jobID)){
         // taskParser.parse(job, p);
          System.out.println("\tTask path : "+ p.toString());
        }
      }
    }

  }



  public void gatherSucc(){
    setChanged();
    notifyObservers();
  }
}
