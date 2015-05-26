package bean;

import java.util.List;

/**
 * Created by zhangyun on 5/18/15.
 */
public class Job {
  private String jobid;
  private String workflowID;
  private String workflowNode;
  private String logPath;
  private String detailInfo;
  private String remark;
  private List<Task> taskList;
  private int taskNum;

  public Job(String jobid, String workflowID, String workflowNode,
        String logPath, String detailInfo, List<Task> taskList) {
    this.jobid = jobid;
    this.workflowID = workflowID;
    this.workflowNode = workflowNode;
    this.logPath = logPath;
    this.detailInfo = detailInfo;
    this.taskList = taskList;
  }

  public Job(String jobid, String workflowID, String workflowNode,
             String logPath, String detailInfo){
    this(jobid, workflowID, workflowNode, logPath, detailInfo, 0, "" );
  }

  public Job(String jobid, String workflowID, String workflowNode,
             String logPath, String detailInfo, int taskNum) {
    this(jobid, workflowID, workflowNode, logPath, detailInfo, taskNum, "" );
  }


  public Job(String jobid, String workflowID, String workflowNode,
         String logPath, String detailInfo, int taskNum, String remark) {
    this.jobid = jobid;
    this.workflowID = workflowID;
    this.workflowNode = workflowNode;
    this.logPath = logPath;
    this.detailInfo = detailInfo;
    this.remark = remark;
    this.taskNum = taskNum;
  }

  public Job() {
  }

  public String getJobid() {
    return jobid;
  }

  public void setJobid(String jobid) {
    this.jobid = jobid;
  }

  public String getWorkflowID() {
    return workflowID;
  }

  public void setWorkflowID(String workflowID) {
    this.workflowID = workflowID;
  }

  public String getWorkflowNode() {
    return workflowNode;
  }

  public void setWorkflowNode(String workflowNode) {
    this.workflowNode = workflowNode;
  }

  public String getLogPath() {
    return logPath;
  }

  public void setLogPath(String logPath) {
    this.logPath = logPath;
  }

  public String getDetailInfo() {
    return detailInfo;
  }

  public void setDetailInfo(String detailInfo) {
    this.detailInfo = detailInfo;
  }

  public String getRemark() {
    return remark;
  }

  public void setRemark(String remark) {
    this.remark = remark;
  }

  public List<Task> getTaskList() {
    return taskList;
  }

  public void setTaskList(List<Task> taskList) {
    this.taskList = taskList;
  }

  public int getTaskNum() {
    return taskNum;
  }

  public void setTaskNum(int taskNum) {
    this.taskNum = taskNum;
  }
}
