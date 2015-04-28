package base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Query {
  private String workflowID;
  private String queryString;
  private List<Job> jobs;
  private HashMap<String,String> workflowAdjacencies;

  private boolean done = false;

  //operator tree

  public Query(String queryString ,String workflowID , String workflowAdjacencies) {
    this.jobs = new ArrayList<Job>();
    this.queryString = queryString;
    this.workflowID = workflowID;
    this.workflowAdjacencies = new HashMap<String, String>();
    parse( workflowAdjacencies );
  }

  public void addJob( Job job){
    this.jobs.add(job);
  }

  public List<Job> getJobs() {
    return jobs;
  }

  private void parse(String workflowAdjacencies){
    String[] groups = workflowAdjacencies.split("\\s+");
    for(String g : groups){
      String[] elements = g.split("=");
      this.workflowAdjacencies.put(elements[0],elements[1]);
    }
  }

  public HashMap<String, String> getWorkflowAdjacencies() {
    return workflowAdjacencies;
  }

  public String getQueryString() {
    return queryString;
  }

  /**
   * this is a flag . when all job logs and task logs of query has
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
