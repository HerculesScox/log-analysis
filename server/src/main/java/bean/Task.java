package bean;

/**
 * Created by zhangyun on 5/18/15.
 */
public class Task {
  private String taskid;
  private String taskType;
  private String logPath;
  private String detailInfo;
  private String remark;

  public Task(String taskid, String taskType, String logPath, String detailInfo) {
    this.taskid = taskid;
    this.taskType = taskType;
    this.logPath = logPath;
    this.detailInfo = detailInfo;
  }

  public Task(String taskid, String taskType, String logPath,
        String detailInfo, String remark) {
    this.taskid = taskid;
    this.taskType = taskType;
    this.logPath = logPath;
    this.detailInfo = detailInfo;
    this.remark = remark;
  }

  public Task() {
  }

  public String getTaskid() {
    return taskid;
  }

  public void setTaskid(String taskid) {
    this.taskid = taskid;
  }

  public String getTaskType() {
    return taskType;
  }

  public void setTaskType(String taskType) {
    this.taskType = taskType;
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
}
