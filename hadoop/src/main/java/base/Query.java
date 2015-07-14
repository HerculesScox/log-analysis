package base;

import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Query  implements Serializable {
  private String workflowID;
  private String queryString;
  private List<Job> jobs;
  private String workflowAdjacencies;
  // the First job launch time
  private long launchTime;
  private boolean done = false;

  //operator tree

  public Query(String queryString ,String workflowID ,
               String workflowAdjacencies)throws IOException {
    this.jobs = new ArrayList<Job>();
    this.queryString = queryString;
    this.workflowID = workflowID;
    this.launchTime = 0L;
    this.workflowAdjacencies = workflowAdjacencies;
  }

  public void addJob( Job job){
    long jobLaunch =  job.getJobInfo().getLaunchTime();
    if(jobs.size() > 0 && this.launchTime > jobLaunch ) {
      this.launchTime = jobLaunch;
    }else{
      this.launchTime = jobLaunch;
    }
    this.jobs.add(job);
  }

  public List<Job> getJobs() {
    return jobs;
  }

  public String parseStages() throws IOException{

    JSONObject jobJson = new JSONObject();
    String[] groups = workflowAdjacencies.split("\\s+");
    for(String g : groups){
      String[] elements = g.split("=");
      String[] stages = elements[1].replace(",","\",\"").split(",");

      if( stages.length == 1){
        jobJson.put(stageToJob(elements[0]), stageToJob(stages[0]));
      }else {
        StringBuilder builder = new StringBuilder();
        builder.append("\"");
        int i = 1;
        for (String stage : stages) {
          builder.append(stageToJob(stage).replace("\"", ""));
          if( i++ < stages.length ) {
            builder.append(",");
          }
        }
        builder.append("\"");
        jobJson.put(stageToJob(elements[0]),builder.toString());
      }
    }
    StringWriter out = new StringWriter();
    jobJson.writeJSONString(out);
    return jobJson.toJSONString();
  }

  private String stageToJob(String stage){
    float runningTime = -1;
    String trimStr = stage.substring(1, stage.length()-1);
    for(Job job : jobs){
      if(job.getJobInfo().getWorkflowNodeName().equals(trimStr)){
        runningTime = (job.getJobInfo().getFinishTime() - job.getJobInfo().getLaunchTime()) / 1000;
        break;
      }
    }

    StringBuilder builder = new StringBuilder();
    builder.append("\"");
    builder.append(trimStr);
    if( runningTime != -1) {
      builder.append("(");
      builder.append(String.valueOf(runningTime));
      builder.append("s)");
    }
    builder.append("\"");
    return builder.toString();
  }

  public String getWorkflowAdjacencies() {
    return workflowAdjacencies;
  }

  public String getQueryString() {
    return queryString;
  }

  /**
   * this is a status flag . when all job logs and task logs of query has
   * finished analyze,the flag is true , or false
   * @return
   */
  public boolean isDone() {
    return done;
  }

  public String getWorkflowID() {
    return workflowID;
  }

  public long getLaunchTime() {
    return launchTime;
  }

  public void setLaunchTime(long launchTime) {
    this.launchTime = launchTime;
  }
}
