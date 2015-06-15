package parse;

import base.Job;
import base.MapTask;
import base.Node;
import base.ReduceTask;
import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import util.PatternParse;
import util.Tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
    opSet.put("MapOperator", "MAP");
    opSet.put("ReduceSinkOperator", "RS");
    opSet.put("TableScanOperator", "TS");
    opSet.put("FilterOperator", "FIL");
    opSet.put("SelectOperator", "SEL");
    opSet.put("GroupByOperator", "GBY");
    opSet.put("JoinOperator", "JOIN");
    opSet.put("FileSinkOperator", "FS");
    opSet.put("ExtractOperator","EX");
    opSet.put("UnionOperator","UNION");
    opSet.put("LimitOperator", "LIM");
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

  public HashSet<String> parse(Job job ,Path taskLog) throws  IOException,ParseException {
    HashSet<String> taskGroup = new HashSet<String>();
    org.apache.hadoop.fs.FileSystem fs = taskLog.getFileSystem(conf);
    FSDataInputStream in = fs.open(taskLog);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    Map<String,JhistFileParser.TaskInfo> tasks = job.getJobInfo().getTasksMap();
    String line;

    //data format of log
    DateFormat fmtDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    //<Operator Name -> <first forwarding time , last forwarding time>>
    HashMap<String, Tuple<Long,Long>> opToProcTime =
            new HashMap<String, Tuple<Long, Long>>();

    //input files format of Map tasks
    String mapInputFormat = "";
    //Store split files paths of map task
    List<String> mapSplitFiles = new ArrayList<String>();
    //Store input task(map) of reduce task
    HashSet<TaskAttemptID> attemptTaskID = new HashSet<TaskAttemptID>();

    //Store all operator of each task , it will be clear
    //after analyzing log of one task
    LinkedHashMap<String, Node> operators = new LinkedHashMap<String, Node>();

    //The following status variables are used , in order to reduce
    //pattern match times
    //To identify task type;
    boolean isMapTask = false ,isRedTask = false;
    //To identify finished of analyzing operator
    boolean opsEnd = false;
    //To identify operator process finished
    boolean opProcFinish = false;
    //To identify operator forwarding status
    boolean isForwarding = false;

    while ((line = br.readLine()) != null) {
      /**One line just matched one pattern, when find them. The
       * remain pattern can be skipped.*/
      String matchedStr = null;
      Matcher matcher = null;
      if ((matcher = PatternParse.mapTaskInput(line)) != null) {
        for (String s : matcher.group(1).split(",")){
          mapSplitFiles.add(s);
        }
        mapInputFormat = matcher.group(2);
        //initialize status value of each map task
        isMapTask = true;
        continue;
      }

      if (!isRedTask &&(matchedStr = PatternParse.reduceTaskInput(line)) != null) {
        for (String ati :matchedStr.split(",") ){
          attemptTaskID.add(TaskAttemptID.forName(ati));
        }
        continue;
      }

      if (PatternParse.reduceOps(line) != null) {
        isRedTask = true;
        continue;
      }

      //Processing Map task log
      if (isMapTask) {
        if ( !opsEnd && PatternParse.mapOps(line) != null) {
          //initialize status value of each map task
          opsEnd = false;
          continue;
        }

        Tuple<String,String> tuple;
        if ( !opsEnd && (tuple = PatternParse.operator(line)) != null ) {
          Node node = new Node(tuple.getKey(), tuple.getValue());
          if(tuple.getKey().equals("MAP")){
            job.getTopOps().add(node);
          }
          operators.put(tuple.getKey(), node);
          continue;
        }

        if ( !opsEnd && PatternParse.endOperator(line)){
          opsEnd = true;
          opProcFinish = false;
          isForwarding= true;
          continue;
        }

        if( isForwarding && (matcher = PatternParse.opForwarding(line)) != null){
          String opName = opSet.get(matcher.group(2));
          String procTime = matcher.group(1);
          long time = fmtDateTime.parse(procTime).getTime();
          if(opToProcTime.containsKey(opName)){
            Tuple<Long ,Long> execTime = opToProcTime.get(opName);
            execTime.setValue(time);
          }else{
            opToProcTime.put(opName,new Tuple<Long, Long>(time, 0L));
          }
        }

        if ( PatternParse.opProcFinish(line) != null){
          opProcFinish = true;
          isForwarding = false;
          continue;
        }

        if (opProcFinish && (matcher = PatternParse.opProcRows(line)) != null){
          String opName = opSet.get(matcher.group(1));
          long rows = Long.valueOf(matcher.group(3));
          if(operators.containsKey(opName)) {
            operators.get(opName).setRows(rows);
          }
          continue;
        }

        if ((matchedStr = PatternParse.taskID(line)) != null) {
          String taskID = "task_" + matchedStr;
          JhistFileParser.TaskInfo taskInfo = job.getJobInfo().getTasksMap().get(taskID);
          Tuple<Long, Long> execTime = null;
          for(String opName : operators.keySet()){
             execTime = opToProcTime.get(opName);
             if (execTime != null) {
               long res ;
               if( execTime.getValue() == 0L ){
                 res = -1L;
               }else{
                res = execTime.getValue() - execTime.getKey();
               }
               operators.get(opName).setProcTime(res);
             }else {
               operators.get(opName).setProcTime(0);
             }
          }
          MapTask mapTask = new MapTask(taskInfo, taskLog, operators,
                  mapSplitFiles, mapInputFormat);
          job.getTasks().put(taskID, mapTask);
          taskGroup.add(taskID);
          LOG.info("Create A Map Task " + taskID);
          //container and status value will be reset
          operators.clear();
          mapSplitFiles.clear();
          opToProcTime.clear();
          mapInputFormat = "";
          opsEnd = false;
          isMapTask = false;
          continue;
        }
      }

      //Processing Reduce task log
      if (isRedTask) {
        if (!opsEnd &&  PatternParse.reduceOps(line) != null) {
          //initialize status value of each map task
          opProcFinish = false;
          continue;
        }
        Tuple<String,String> tuple;
        if (!opsEnd && (tuple = PatternParse.operator(line)) != null ) {
          Node node = new Node(tuple.getKey(), tuple.getValue());
          operators.put(tuple.getKey(), node);
          continue;
        }
        if (!opsEnd && PatternParse.endOperator(line) ){
          opsEnd = true;
          opProcFinish = false;
          isForwarding= true;
          continue;
        }

        if( isForwarding && (matcher = PatternParse.opForwarding(line)) != null){
          String opName = opSet.get(matcher.group(2));
          String procTime = matcher.group(1);
          long time = fmtDateTime.parse(procTime).getTime();
          if(opToProcTime.containsKey(opName)){
            Tuple<Long ,Long> execTime = opToProcTime.get(opName);
            execTime.setValue(time);
          }else{
            opToProcTime.put(opName,new Tuple<Long, Long>(time, 0L));
          }
        }

        if (PatternParse.opProcFinish(line) != null){
          opProcFinish = true;
          isForwarding = false;
          continue;
        }

        if (opProcFinish &&(matcher = PatternParse.opProcRows(line)) != null){
          String opName = opSet.get(matcher.group(1));
          long rows = Long.valueOf(matcher.group(3));
          operators.get(opName).setRows(rows);
          continue;
        }

        //extract FileSinkOperator
        if ((matchedStr = PatternParse.FSpath(line)) != null){
          operators.get("FS").setRemark(matchedStr);
          continue;
        }

        if ((matchedStr = PatternParse.taskID(line)) != null) {
          String taskID = "task_" + matchedStr;
          JhistFileParser.TaskInfo taskInfo = job.getJobInfo().getTasksMap().get(taskID);
          Tuple<Long, Long> execTime = null;

          for(String opName : operators.keySet()){
            execTime = opToProcTime.get(opName);
            if (execTime != null) {
              operators.get(opName).setProcTime(execTime.getValue() - execTime.getKey());
            }
            //FileSInkOperator and JoinOperator may be ignored.
            //continue;
          }

          ReduceTask reduceTask= new ReduceTask(taskInfo, taskLog, operators, attemptTaskID);
          LOG.info("Create A Reduce Task " + taskID);
          job.getTasks().put(taskID,reduceTask);
          taskGroup.add(taskID);
       // container and status value will be reset
          operators.clear();
          attemptTaskID.clear();
          opToProcTime.clear();
          opsEnd = false;
          isRedTask = false;
        }
      }
    }

    return taskGroup;
  }
}
