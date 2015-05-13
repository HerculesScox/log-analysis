package base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.json.simple.JSONObject;
import parse.JhistFileParser;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Created by zhangyun on 4/21/15.
 */
public abstract class Task {
  private static Log LOG = LogFactory.getLog(Task.class);
  protected String taskID;
  protected Path taskLogPath;
  protected JhistFileParser.TaskInfo taskInfo;
  /**Key is operator identifier, The value is operator object*/
  protected LinkedHashMap<String,Node> operators;


  public Task(Path taskLogPath, JhistFileParser.TaskInfo taskInfo,
              LinkedHashMap<String, Node> operators) {
    this.taskID = taskInfo.getTaskId().toString();
    this.taskLogPath = taskLogPath;
    this.taskInfo = taskInfo;
    this.operators = new LinkedHashMap<String, Node>();
    this.operators.putAll(operators);
  }

  public String getTaskID() {
    return taskID;
  }

  public Path getTaskLogPath() {
    return taskLogPath;
  }

  public JhistFileParser.TaskInfo getTaskInfo() {
    return taskInfo;
  }

  public LinkedHashMap<String, Node> getOperators() {
    return operators;
  }

  public void printAll(){
    System.out.println("TASK_ID: " + taskID);
//    for (String g : taskInfo.getCounters().getGroupNames()){
//      Iterator<org.apache.hadoop.mapreduce.Counter> c =
//              taskInfo.getCounters().getGroup(g).iterator();
//      while (c.hasNext()) {
//        org.apache.hadoop.mapreduce.Counter n = c.next();
//        System.out.println(g + ">>>>>>>>>> " + n.getName() + " => " + n.getValue());
//      }
//      System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
//    }

    System.out.println(taskInfo.getTaskType());
    System.out.println(taskInfo.getAllTaskAttempts().get(taskInfo.getSuccessfulAttemptId()).getState());

//    System.out.println("OPTREE: ");
//    for (String k : operators.keySet()){
//      System.out.println(k + " => " + operators.get(k).toString());
//    }
//    System.out.println("LOG_PATH: "+ taskLogPath);
  }

  public String toJSON() throws IOException {
    JSONObject jobJson = new JSONObject();
    jobJson.put("taskID", taskID);

    StringWriter out = new StringWriter();
    jobJson.writeJSONString(out);

    return jobJson.toJSONString();
    }

}