package base;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.json.simple.JSONObject;
import parse.JhistFileParser;
import util.FactorStatistics;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * Created by zhangyun on 5/6/15.
 */
public class ReduceTask extends Task {
  //All input tasks of reduce task
  private HashSet<TaskAttemptID> attemptTaskIDs;

  public ReduceTask(JhistFileParser.TaskInfo taskInfo,Path taskLogPath,
                    LinkedHashMap<String, Node> operators, HashSet<TaskAttemptID> attemptTaskID){
    super(taskLogPath, taskInfo, operators);
    this.attemptTaskIDs = new HashSet<TaskAttemptID>();
    getAttemptTaskID().addAll(attemptTaskID);
  }

  public HashSet<TaskAttemptID> getAttemptTaskID() {
    return attemptTaskIDs;
  }

  public void setAttemptTaskIDs(HashSet<TaskAttemptID> attemptTaskIDs) {
    this.attemptTaskIDs = attemptTaskIDs;
  }

  public void printAll(){
    System.out.println("TASK_ID: " + taskID);
    System.out.println(taskInfo.getTaskType());
    System.out.println("INPUT TASKS:");
    for(TaskAttemptID ta : attemptTaskIDs) {
      System.out.println(ta.getTaskID() + "[" + ta.getTaskType() + "]");
    }

    System.out.println("OPTREE: ");
    for (String k : operators.keySet()){
      System.out.println(k + " => " + operators.get(k).toString());
    }
    taskInfo.printAll();
  }


  public String toJSON() throws IOException {
    JSONObject jobJson = new JSONObject();

    LinkedList<String> inputOfRedTask = new LinkedList<String>();
    for(TaskAttemptID ta : attemptTaskIDs){
      inputOfRedTask.add(ta.getTaskID().toString());
    }
    setJSONObject(jobJson);

    jobJson.put("inputMapTasks", inputOfRedTask);
    jobJson.put("attemptTask", taskInfo.getAllTaskAttempts().keySet().toString());

    LinkedHashMap redcounter = new LinkedHashMap();
    FactorStatistics.reduceCounter(redcounter, taskInfo.getCounters());
    jobJson.put("Counter",redcounter);

    StringWriter out = new StringWriter();
    jobJson.writeJSONString(out);

    return jobJson.toJSONString();
  }
}
