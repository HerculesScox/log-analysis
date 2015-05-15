package base;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.json.simple.JSONObject;
import parse.JhistFileParser;
import util.FactorStatistics;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Job implements Serializable{
  private JhistFileParser.JobInfoQ  jobInfo;
  private Path JHPath;
  private Path confFile;
  private HashMap<TaskID, Task> tasks;
  //All top op of all map tasks
  private HashSet<Node> topOps;


  public Job(JhistFileParser.JobInfoQ jobInfo , Path JHPath , Path confFile) {
    this.jobInfo = jobInfo;
    this.JHPath = JHPath;
    this.confFile = confFile;
    this.topOps = new HashSet<Node>();
    this.tasks = new HashMap<TaskID, Task>();
  }

  public Path getConfFile() {
    return confFile;
  }

  public Path getJHPath() {
    return JHPath;
  }

  public JhistFileParser.JobInfoQ getJobInfo() {
    return jobInfo;
  }

  public HashMap<TaskID, Task> getTasks() {
    return tasks;
  }

  public HashSet<Node> getTopOps() {
    return topOps;
  }

  public void chopKilledTask(){
    Set<TaskID> succTask = tasks.keySet();
    HashSet<TaskAttemptID> redInputTask = new HashSet<TaskAttemptID>();
    for(TaskID id : tasks.keySet()){
      if (tasks.get(id) instanceof ReduceTask){
        ReduceTask rt = (ReduceTask) tasks.get(id);
        for(TaskAttemptID ta : rt.getAttemptTaskID()){
          if(succTask.contains(ta.getTaskID())){
            redInputTask.add(ta);
          }
        }
        rt.setAttemptTaskIDs(redInputTask);
      }
    }
  }


  public String toJSON() throws IOException{
    JSONObject jobJson = new JSONObject();
    jobJson.put("jobID", jobInfo.getJobid().toString());
    jobJson.put("workflowID", jobInfo.getWorkflowId());
    jobJson.put("workflowNodeName", jobInfo.getWorkflowNodeName());
    jobJson.put("logPath", confFile);
    jobJson.put("submitTime", jobInfo.getSubmitTime());
    jobJson.put("launchTime", jobInfo.getLaunchTime());

    LinkedHashMap counterGroup = new LinkedHashMap();
    LinkedHashMap mapcounter = new LinkedHashMap();
    LinkedHashMap redcounter = new LinkedHashMap();
    LinkedHashMap totalcounter = new LinkedHashMap();

    FactorStatistics.mapCounter(mapcounter, jobInfo.getMapCounters());
    counterGroup.put("mapCounter",mapcounter);
    FactorStatistics.reduceCounter(redcounter, jobInfo.getReduceCounters());
    counterGroup.put("redCounter",redcounter);
    FactorStatistics.totalCounter(totalcounter, jobInfo.getTotalCounters());
    counterGroup.put("totalCounter",totalcounter);

    jobJson.put("Counter",counterGroup);

    StringWriter out = new StringWriter();
    jobJson.writeJSONString(out);

    return jobJson.toJSONString();
  }
}
