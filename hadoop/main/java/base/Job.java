package base;

import org.apache.hadoop.fs.Path;
import parse.JhistFileParser;

import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Job {
  private JhistFileParser.JobInfoQ  jobInfo;
  private Path JHPath;
  private Path confFile;
  // JobId map to all path of corresponding task log files
  private HashSet<Path> taskLogPath;
  private HashSet<Task> tasks;
  private String generateDate;

  public Job(JhistFileParser.JobInfoQ jobInfo , Path JHPath , Path confFile) {
    this.jobInfo = jobInfo;
    this.JHPath = JHPath;
    this.confFile = confFile;
    this.taskLogPath = new HashSet<Path>();
    this.tasks = new HashSet<Task>();
    this.generateDate = Calendar.getInstance().getTime().toString();
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

  public String getWorkflowName() {
    return jobInfo.getWorkflowName();
  }

  public String getWorkflowNodeName() {
    return jobInfo.getWorkflowNodeName();
  }

  public void setTaskLogPath(HashSet<Path> taskLogPath) {
    this.taskLogPath = taskLogPath;
  }

  public void setTasks(HashSet<Task> tasks) {
    this.tasks = tasks;
  }
}
