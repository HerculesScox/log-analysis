package base;

import org.apache.hadoop.fs.Path;
import parse.JhistFileParser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by zhangyun on 4/29/15.
 */
public class MapTask extends Task {
  /**The MapTask processing split, input file paths are split file paths*/
  private List<String> splitFiles;
  private String inputFormat;

  public MapTask( JhistFileParser.TaskInfo taskInfo, Path taskLogPath,
                 LinkedHashMap<String, Node> operators ,List<String> splitFiles,
                 String inputFormat) {
    super(taskLogPath, taskInfo, operators);
    this.splitFiles = new ArrayList<String>();
    this.splitFiles.addAll(splitFiles);
    this.inputFormat = inputFormat;
  }

  public void printAll(){
    System.out.println("TASK_ID: " + taskID);
    System.out.println(taskInfo.getTaskType());
    System.out.println("INPUT FILES:");
    System.out.println(splitFiles);
    System.out.println("INPUT FORMAT: " + inputFormat );
    System.out.println("OPTREE: ");
    for (String k : operators.keySet()){
      System.out.println(k + " => " + operators.get(k).toString());
    }
  }
}
