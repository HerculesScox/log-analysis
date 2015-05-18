package bean;

import java.util.List;

/**
 * Created by zhangyun on 5/18/15.
 */
public class Query {
  private String queryStirng;
  private String workflowID;
  private String jobDependency;
  private String username;
  private List<Job> jobList;
  private String remark;


  public Query(String queryStirng, String username, String jobDependency,
          String workflowid, List<Job> jobList) {
    this.queryStirng = queryStirng;
    this.username = username;
    this.jobDependency = jobDependency;
    this.workflowID = workflowid;
    this.jobList = jobList;
  }

  public Query(String queryStirng, String workflowid, String jobDependency,
          String username, String remark) {
    this.queryStirng = queryStirng;
    this.workflowID = workflowid;
    this.jobDependency = jobDependency;
    this.username = username;
    this.remark = remark;
  }

  public Query(String queryStirng, String workflowID, String jobDependency,
           String username) {
    this.queryStirng = queryStirng;
    this.workflowID = workflowID;
    this.jobDependency = jobDependency;
    this.username = username;
  }

  public Query() {

  }

  public String getQueryStirng() {
    return queryStirng;
  }

  public void setQueryStirng(String queryStirng) {
    this.queryStirng = queryStirng;
  }

  public String getWorkflowid() {
    return workflowID;
  }

  public void setWorkflowid(String workflowid) {
    this.workflowID = workflowid;
  }

  public String getJobDependency() {
    return jobDependency;
  }

  public void setJobDependency(String jobDependency) {
    this.jobDependency = jobDependency;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getRemark() {
    return remark;
  }

  public void setRemark(String remark) {
    this.remark = remark;
  }

  public List<Job> getJobList() {
    return jobList;
  }

  public void setJobList(List<Job> jobList) {
    this.jobList = jobList;
  }

}
