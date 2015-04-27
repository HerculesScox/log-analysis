package parse;

import base.Job;
import base.Query;
import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import util.PathParse;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyun on 4/27/15.
 */
public class JHParser {
  private static Log LOG = LogFactory.getLog(JHParser.class);
  private LogInput logInput;
  private LAConf conf;
  private HashMap<String,Query> queries;

  public JHParser(LAConf conf) {
    this.conf = conf;
    logInput = new LogInput(conf);
    queries = new HashMap<String,Query>();
  }

  public HashMap<String,Path> parseXMLFilePath() throws IOException{
    HashSet<Path> paths = logInput.genXFPath();
    HashMap<String,Path> res = new HashMap<String, Path>();
    for (Path path : paths){
      String jobID = PathParse.jobAndID(path.toString());
      res.put(jobID,path);
    }
    return res;
  }

  public void ParseJsonFile( ) throws IOException {
    HashSet<Path> jpaths = logInput.genJFPath();
    HashMap<String,Path> xmlFiles = parseXMLFilePath();
    for (Path path : jpaths){
      FileSystem fs = path.getFileSystem(conf);
      JhistFileParser jp = new JhistFileParser( fs, path);
      JhistFileParser.JobInfoQ jobInfo = jp.parse();

      Job job = new Job(jobInfo , path ,xmlFiles.get(jobInfo.getJobid().toString()));
      if( ! queries.keySet().contains(jobInfo.getWorkflowId())){
        Query q = new Query(jobInfo.getWorkflowName(),
                jobInfo.getWorkflowId(),jobInfo.getWorkflowAdjacencies());
        q.addJob(job);
        queries.put(jobInfo.getWorkflowId(),q);
      }else{
        queries.get(jobInfo.getWorkflowId()).addJob(job);
      }
    }
  }

  public HashMap<String, Query> getQueries() {
    return queries;
  }

  public static void main(String[] args ){
    LAConf conf = new LAConf();
    try{
      JHParser pad = new JHParser(conf);
      Job  tmpJob = null;
      pad.ParseJsonFile();

      for( String key : pad.getQueries().keySet()){
        System.out.println(">>workFlowID :" + key);
    //    System.out.println(">>Query : \n"+ pad.getQueries().get(key).getQueryString() );
        List<Job> jobs = pad.getQueries().get(key).getJobs();
        for(Job j : jobs){
          tmpJob = j ; //test
          System.out.println(">> JOB INFO :" );
          System.out.println(j.getJobInfo().getJobid());
        }

        for(String k :  pad.getQueries().get(key).getWorkflowAdjacencies().keySet()){
          System.out.println( k + " => "+
                  pad.getQueries().get(key).getWorkflowAdjacencies().get(k));
        }
      }

      System.out.println(" ============================================ ");
//      Path path = tmpJob.getJHPath();
//      FileSystem fs = path.getFileSystem(conf);
//      JobHistoryParser jp = new JobHistoryParser( fs,path);
//      for( Map.Entry<TaskID, JobHistoryParser.TaskInfo> task : jp.parse().getAllTasks().entrySet()){
//        System.out.println("task = " + task.getKey() +"\n value = ");
//        task.getValue().printAll();
//      }
      System.out.println(" ============================================ ");



    }catch (IOException e){
      e.printStackTrace();
    }
  }
}
