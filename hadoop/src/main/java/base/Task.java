package base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.json.simple.JSONObject;
import parse.JhistFileParser;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by zhangyun on 4/21/15.
 */
public abstract class Task  implements Serializable {
  private static Log LOG = LogFactory.getLog(Task.class);
  protected long startProcTime;
  protected long doneProcTime;
  protected String taskID;
  protected Path taskLogPath;
  protected JhistFileParser.TaskInfo taskInfo;
  /**Key is operator identifier, The value is operator object*/
  protected LinkedHashMap<String,Node> operators;


  public Task(Path taskLogPath, JhistFileParser.TaskInfo taskInfo,
              LinkedHashMap<String, Node> operators, long startProcTime,
              long doneProcTime) {
    this.taskID = taskInfo.getTaskId().toString();
    this.taskLogPath = taskLogPath;
    this.taskInfo = taskInfo;
    this.operators = new LinkedHashMap<String, Node>();
    this.operators.putAll(operators);
    this.startProcTime = startProcTime;
    this.doneProcTime = doneProcTime;
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
    System.out.println(taskInfo.getTaskType());
    System.out.println(taskInfo.getAllTaskAttempts().get(taskInfo.getSuccessfulAttemptId()).getState());

  }

  public String toJSON()throws IOException{
   return "{\"taskID\": \""+ taskID +"\"}";
  }

  public void setJSONObject(JSONObject jobJson) throws IOException {
    jobJson.put("taskID", taskID.toString());

    LinkedHashMap<String,String> ops = new LinkedHashMap<String,String>();
    for(String k : operators.keySet()){
      if( operators.get(k).getRemark() != null){
        ops.put( k , operators.get(k).toString() +
                " <" + operators.get(k).getRemark() + ">") ;
        continue;
      }
      ops.put( k , operators.get(k).toString());
    }
    jobJson.put("operatorTree", ops);

    jobJson.put("startTime", startProcTime);
    jobJson.put("finishTime", doneProcTime);
    jobJson.put("splitLocation", taskInfo.getSplitLocations());
    jobJson.put("error", taskInfo.getError());
    jobJson.put("status", taskInfo.getTaskStatus());

    Map<TaskAttemptID, JhistFileParser.TaskAttemptInfo> tas = taskInfo.getAllTaskAttempts();
    LinkedHashMap attemptTasks = new LinkedHashMap();
    for (TaskAttemptID at : tas.keySet()){
      LinkedHashMap attemptTask = new LinkedHashMap();
      attemptTask.put("startTime", tas.get(at).getStartTime());
      attemptTask.put("finishTime", tas.get(at).getFinishTime());
      attemptTask.put("error", tas.get(at).getError());
      attemptTask.put("status", tas.get(at).getTaskStatus());
      attemptTask.put("state", tas.get(at).getState());
      attemptTask.put("type", tas.get(at).getTaskType().toString());
      attemptTask.put("shuffleFinishTime", tas.get(at).getShuffleFinishTime());
      attemptTask.put("sortFinishTime", tas.get(at).getSortFinishTime());
      attemptTask.put("mapFinishTime", tas.get(at).getMapFinishTime());
      attemptTask.put("containerId", tas.get(at).getContainerId().toString());
      attemptTasks.put(at.toString(), attemptTask);
    }
    jobJson.put("attemptTasks", attemptTasks);
  }

}