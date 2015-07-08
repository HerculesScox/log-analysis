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
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Job implements Serializable{
  private JhistFileParser.JobInfoQ  jobInfo;
  private Path JHPath;
  private Path confFile;
  private HashMap<String, Task> tasks;
  //All top op of all map tasks
  private HashSet<Node> topOps;


  public Job(JhistFileParser.JobInfoQ jobInfo , Path JHPath , Path confFile) {
    this.jobInfo = jobInfo;
    this.JHPath = JHPath;
    this.confFile = confFile;
    this.topOps = new HashSet<Node>();
    this.tasks = new HashMap<String, Task>();
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

  public HashMap<String, Task> getTasks() {
    return tasks;
  }

  public HashSet<Node> getTopOps() {
    return topOps;
  }

  public void chopKilledTask(){
    Set<String> succTask = tasks.keySet();
    HashSet<TaskAttemptID> redInputTask = new HashSet<TaskAttemptID>();
    for(String id : tasks.keySet()){
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
    jobJson.put("submitTime", jobInfo.getSubmitTime());
    jobJson.put("launchTime", jobInfo.getLaunchTime());
    jobJson.put("finishTime", jobInfo.getFinishTime());

    List<String> mapInputs = new ArrayList<String>();
    List<String> mapInputFormat = new ArrayList<String>();
    List<String> redInputs = new ArrayList<String>();
    LinkedHashMap<String, BigDecimal> mapOps = new LinkedHashMap<String, BigDecimal>();
    LinkedHashMap<String, BigDecimal> reduceOps = new LinkedHashMap<String, BigDecimal>();

    for(Task task : tasks.values()){
      if (task instanceof MapTask){
        for(String node : task.getOperators().keySet()){
          BigDecimal rows = BigDecimal.valueOf(task.getOperators().get(node).getRows());
          if (mapOps.containsKey(node)) {
            mapOps.put(node, mapOps.get(node).add(rows));
          }else{
            mapOps.put(node,rows);
          }
        }

        MapTask mt = (MapTask)task;
        if (!mapInputFormat.contains(mt.getInputFormat())){
          mapInputFormat.add(mt.getInputFormat());
        }
        for(String inputFile : mt.getSplitFiles()){
          String fileName = inputFile.split(":")[0];
          if(!mapInputs.contains(fileName)){
             mapInputs.add(fileName);
          }
        }
      }

      if (task instanceof ReduceTask){

        for(String node : task.getOperators().keySet()){
          BigDecimal rows = BigDecimal.valueOf(task.getOperators().get(node).getRows());
          if (reduceOps.containsKey(node)) {
            reduceOps.put(node, reduceOps.get(node).add(rows));
          }else{
            reduceOps.put(node,rows);
          }
        }

        ReduceTask mt = (ReduceTask)task;
        for (TaskAttemptID at : mt.getAttemptTaskID()) {
          redInputs.add(at.getTaskID().toString());
        }

      }
    }

    jobJson.put("mapInputs", mapInputs);
    jobJson.put("mapInputFormat", mapInputFormat);
    jobJson.put("redInputs", redInputs);
    jobJson.put("reduceOps", reduceOps);
    jobJson.put("mapOps", mapOps);
    System.out.println(jobInfo.getJobid().toString() + " " +redInputs);
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
