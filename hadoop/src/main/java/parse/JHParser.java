package parse;

import base.Job;
import base.Query;
import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.PatternParse;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by zhangyun on 4/27/15.
 */
public class JHParser {
  private static Log LOG = LogFactory.getLog(JHParser.class);
  private LogInput logInput;
  private LAConf conf;
  private HashMap<String, Query> queries;

  public JHParser(LAConf conf) {
    this.conf = conf;
    logInput = new LogInput(conf);
    queries = new HashMap<String, Query>();
  }

  public HashMap<String,Path> parseXMLFilePath() throws IOException{
    HashSet<Path> paths = logInput.genXFPath();
    HashMap<String,Path> res = new HashMap<String, Path>();
    for (Path path : paths){
      String jobID = PatternParse.jobAndID(path.toString());
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
      LOG.debug("Generate a new job "+jobInfo.getJobid().toString() +"] ."+
              "The history log file path: " + path +". configuration file paht : "+
              xmlFiles.get(jobInfo.getJobid().toString()));
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
}
