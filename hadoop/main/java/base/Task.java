package base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultiset;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by zhangyun on 4/21/15.
 */
public abstract class Task {
  private static Log LOG = LogFactory.getLog(Task.class);
  private String taskID;
  private Path taskLogPath;
  private JobHistoryParser.TaskInfo taskInfo;
  /**Key is operator identifier, The value is operator object*/
  private LinkedHashMap<String,Node> operators;


  public Task(Path taskLogPath, JobHistoryParser.TaskInfo taskInfo,
              LinkedHashMap<String, Node> operators) {
    this.taskID = taskInfo.getTaskId().toString();
    this.taskLogPath = taskLogPath;
    this.taskInfo = taskInfo;
    this.operators = operators;
    this.operators = new LinkedHashMap<String, Node>();
  }

  public String getTaskID() {
    return taskID;
  }

  public Path getTaskLogPath() {
    return taskLogPath;
  }

  public JobHistoryParser.TaskInfo getTaskInfo() {
    return taskInfo;
  }

  public LinkedHashMap<String, Node> getOperators() {
    return operators;
  }
}