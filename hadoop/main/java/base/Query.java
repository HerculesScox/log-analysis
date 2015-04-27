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
  private HashMap<String,String> queryConf;

  public Query(String queryString ,String workflowID , String workflowAdjacencies) {
    this.jobs = new ArrayList<Job>();
    this.queryString = queryString;
    this.workflowID = workflowID;
    this.queryConf = new HashMap<String, String>();
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
}
