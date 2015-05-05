package base;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;

import java.util.HashSet;
import java.util.LinkedHashMap;

/**
 * Created by zhangyun on 4/29/15.
 */
public class MapTask extends Task {
  /**The MapTask processing split, input file paths are split file paths*/
  private String splitFile;

  public MapTask(JobHistoryParser.TaskInfo taskInfo, Path taskLogPath,
                 LinkedHashMap<String, Node> operators) {
    super(taskLogPath, taskInfo, operators);
  }
  public MapTask(Path TaskLogPath){


  }



}
