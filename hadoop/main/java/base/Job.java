package base;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskID;
import parse.JhistFileParser;

import java.util.*;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Job {
  private JhistFileParser.JobInfoQ  jobInfo;
  private Path JHPath;
  private Path confFile;
  private HashMap<TaskID,Task> tasks;
  private String generateDate;
  //All top op of all map tasks
  private HashSet<Node> topOps;

  public Job(JhistFileParser.JobInfoQ jobInfo , Path JHPath , Path confFile) {
    this.jobInfo = jobInfo;
    this.JHPath = JHPath;
    this.confFile = confFile;
    this.topOps = new HashSet<Node>();
    this.tasks = new HashMap<TaskID,Task>();
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


}
