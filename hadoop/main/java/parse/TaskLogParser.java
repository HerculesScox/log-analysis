package parse;

import base.MapTask;
import base.Node;
import conf.LAConf;
import base.Job;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskID;
import util.PatternParse;
import util.Tuple;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;

/**
 * Created by zhangyun on 4/28/15.
 */
public class TaskLogParser {
  private static Log LOG = LogFactory.getLog(JHParser.class);
  private LogInput logInput;
  private LAConf conf;

  private static HashMap<String,String> opSet = new HashMap<String,String>();
  static {
    opSet.put("MAP", "MapOperator");
    opSet.put("RS", "ReduceSinkOperator");
    opSet.put("TS", "TableScanOperatr");
    opSet.put("FIL", "FilterOperator");
    opSet.put("SEL", "SelectOperator");
    opSet.put("GBY", "GroupByOperator");
    opSet.put("JOIN","JoinOperator");
    opSet.put("FS", "FileSinkOperator");
    opSet.put("EX", "ExtractOperator");
    opSet.put("UNION", "UnionOperator");
    //opSet.put("LIM","LimtOperator");
  }
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
    Map<TaskID,JhistFileParser.TaskInfo> tasks = job.getJobInfo().getTasksMap();
    String line;
    LinkedHashMap<String,Node> operators = new LinkedHashMap<String, Node>();
    //To identify task type;
    boolean isMapTask = false ,isRedTask = false;
    //To identify finished of analyzing operator
    boolean opsEnd = false;
    //Start to collect process information of operators
    boolean procFinished = false;

    MapTask mapTask = null;

    while( (line = br.readLine()) != null) {
      String matchedStr = null;
      if ((matchedStr = PatternParse.mapOps(line)) != null) {
        System.out.println(">>>>>>>>>>>>>>>> " + matchedStr);
        //initialize status value of each map task
        isMapTask = true;
        opsEnd = false;
        procFinished = false;
        continue;
      }
      if ((matchedStr = PatternParse.reduceOps(line)) != null) {
        System.out.println(">>>>>>>>>>>>>>>> " + matchedStr);
        isRedTask = true;
        continue;
      }
      if(isMapTask) {
        Tuple<String,String> tuple;
        if ( !opsEnd && (tuple = PatternParse.operator(line)) != null ) {
          System.out.println(">>>>>>>>>>>>>>>> " + matchedStr);
          Node node = new Node(tuple.getKey(), tuple.getValue());
          operators.put(node.toString(), node);
          continue;
        }
        if (PatternParse.endOperator(line) != null){
          opsEnd = true;
          continue;
        }
        Matcher matcher = null;
        if(PatternParse.opProcFinish(line) != null){
          procFinished = true;
          continue;
        }
        if( procFinished && (matcher = PatternParse.opProcRows(line)) != null){
          String opName = matcher.group(1);
          String opID = matcher.group(2);
          long rows = Long.valueOf(matcher.group(3));
          continue;
        }

      }


    }
  }
}
