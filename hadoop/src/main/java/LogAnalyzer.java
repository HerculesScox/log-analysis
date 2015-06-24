import base.Job;
import base.Query;
import conf.LAConf;
import jdbc.DBHander;
import jdbc.Recorder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskID;
import parse.JHParser;
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
      for(Job job : q.getJobs()){
        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        String jobID = job.getJobInfo().getJobid().toString();
        for(Path p : jobIDToTaskPath.get(jobID)){
          job.chopKilledTask();
          HashSet<String> taskGroup =taskParser.parse(job, p);
          for(String id : taskGroup){
            System.out.println(id);
        //    Recorder.taskInfoRecord(job.getTasks().get(id),jobID );
            System.out.println("------------------------------------ ");
          }
        }
   //     Recorder.jobInfoRecord(job);
      }
   //   Recorder.queryInfoRecord(q);
    }
    DBHander.close();
  }

  public void gatherSucc(){
    setChanged();
    notifyObservers();
  }
}
