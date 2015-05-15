package base;

import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Query  implements Serializable {
  private String workflowID;
  private String queryString;
  private List<Job> jobs;
  private String workflowAdjacencies;

  private boolean done = false;

  //operator tree

  public Query(String queryString ,String workflowID ,
               String workflowAdjacencies)throws IOException {
    this.jobs = new ArrayList<Job>();
    this.queryString = queryString;
    this.workflowID = workflowID;
    parse( workflowAdjacencies );
  }

  public void addJob( Job job){
    this.jobs.add(job);
  }

  public List<Job> getJobs() {
    return jobs;
  }

  private void parse(String workflowAdjacencies) throws IOException{
    JSONObject jobJson = new JSONObject();
    String[] groups = workflowAdjacencies.split("\\s+");
    for(String g : groups){
      String[] elements = g.split("=");
      jobJson.put(elements[0],elements[1]);
    }
    StringWriter out = new StringWriter();
    jobJson.writeJSONString(out);
    this.workflowAdjacencies = jobJson.toJSONString();
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
}
