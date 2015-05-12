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
import java.text.ParseException;
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

  public void analyze() throws IOException, ParseException {
    JHParser pad = new JHParser(conf);
    TaskLogParser taskParser = new TaskLogParser(conf);
    HashMap<String,ArrayList<Path>> jobIDToTaskPath = taskParser.parsePath();

    pad.ParseJsonFile();
    Collection<Query> queries = pad.getQueries().values();
    for(Query q : queries){
      System.out.println("================================================");
      System.out.println(">>QUERY "+ q.getQueryString());

      for(Job job : q.getJobs()){
        String jobID = job.getJobInfo().getJobid().toString();
        System.out.println(job.toJSON());
        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        for(Path p : jobIDToTaskPath.get(jobID)){
          taskParser.parse(job, p);
          job.chopKilledTask();
          for(TaskID id : job.getTasks().keySet()){
            System.out.println(id.toString() + " => ");
            job.getTasks().get(id).printAll();
          }
          System.out.println("------------------------------------ ");
        }
        System.out.println("******* Total task numbers: "+ job.getJobInfo().getTasksMap().size());
        System.out.println("*******  Actual task numbers: "+ job.getTasks().size());
        break;
      }
    }

  }

  public void gatherSucc(){
    setChanged();
    notifyObservers();
  }
}
