package util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhangyun on 4/24/15.
 */
public class PatternParse {
  private static Log LOG = LogFactory.getLog(PatternParse.class);
  private static Pattern jobPattern = Pattern.compile("job_\\d+_\\d+");
  private static Pattern jobID = Pattern.compile("_\\d+_\\d+");

  /**Map task log information */
  private static Pattern mapTaskInput = Pattern.compile("org\\.apache\\.hadoop" +
          "\\.mapred\\.MapTask:\\s+Processing\\s+split:\\s+Paths.*$");
  private static Pattern splitPath =
          Pattern.compile("Paths:(/\\w)+:\\d+\\+\\d+,(/\\w)+:\\d+\\+\\d+");
  private static Pattern inputFormat =
          Pattern.compile("InputFormatClass:.*$");
  private static Pattern numReduceTasks =
          Pattern.compile("org\\.apache\\.hadoop" +
                  "\\.mapred\\.MapTask:\\s+numReduceTasks:.*$");
  private static Pattern kvi =
          Pattern.compile("org\\.apache\\.hadoop" +
                  "\\.mapred\\.MapTask:\\s+\\(EQUATOR\\).*$");
  private static Pattern taskSortMB =
          Pattern.compile("org\\.apache\\.hadoop" +
                  "\\.mapred\\.MapTask:\\s+mapreduce\\.task\\.io\\.sort\\.mb:.*$");
  private static Pattern taskSoftLimit =
          Pattern.compile("org\\.apache\\.hadoop" +
                  "\\.mapred\\.MapTask:\\s+soft\\s+limit\\s+at\\s+\\d+$");
  private static Pattern mapTaskBuf =
          Pattern.compile("org\\.apache\\.hadoop" +
                  "\\.mapred\\.MapTask:\\s+bufstart\\s+=\\s+\\d+;\\s+bufvoid.*$");
  private static Pattern mapTaskKv =
          Pattern.compile("org\\.apache\\.hadoop" +
                  "\\.mapred\\.MapTask:\\s+kvstart\\s+=\\s+\\d+;\\s+length.*$");
  private static Pattern mapTaskOutputCollector =
          Pattern.compile("org\\.apache\\.hadoop" +
                  "\\.mapred\\.MapTask:\\s+Map\\s+output\\s+collector.*$");
  private static Pattern operator =
          Pattern.compile("<Parent>.*</Parent>\\s*$|<\\w+>Id =\\d+\\s*$|<\\w+>\\s*$|</\\w+>\\s*$");

  public static String jobAndID( String path ){
    Matcher match = jobPattern.matcher(path);
    if(match.find()){
      return match.group();
    }
    LOG.debug("Can not match job_[n]_[n] pattern in path name of file");
    return null;
  }

  public static String jobID( String path ){
    Matcher match = jobID.matcher(path);
    if(match.find()){
      return match.group();
    }
    LOG.debug("Can not match _[n]_[n] pattern in path name of file");
    return null;
  }


  public static String operator(String str){
    Matcher match = operator.matcher(str);
    if(match.find()){
      return match.group();
    }
    LOG.debug("Can not match <Parent>.*</Parent>\\s*$|<\\w+>Id =\\d+\\s*$|<\\w+>\\s*$|</\\w+>\\s*$ pattern in string");
    return null;
  }

  public static String mapTaskOutputCollector(String str){
    Matcher match = mapTaskOutputCollector.matcher(str);
    if(match.find()){
      String[] mixStr = match.group().split(" ");
      return mixStr[mixStr.length - 1];
    }
    LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: Map output +collector.*$ pattern in string");
    return null;
  }

  public static HashMap<String, Long> mapTaskKv(String str){
    HashMap<String, Long> taskKv = new HashMap<String, Long>();
    Matcher match = mapTaskKv.matcher(str);
    if(match.find()){
      String[] mixStr = match.group().split(" ");
      taskKv.put(mixStr[1], Long.valueOf(mixStr[3].substring(0, mixStr[3].length() - 2)));
      taskKv.put(mixStr[4],Long.valueOf(mixStr[6]));

    }else {
      LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: kvstart = \\d+; length.*$ pattern in string");
    }
    return taskKv;
  }

  public static HashMap<String, Long> mapTaskBuf(String str){
    HashMap<String, Long> taskBuf = new HashMap<String, Long>();
    Matcher match = mapTaskBuf.matcher(str);
    if(match.find()){
      String[] mixStr = match.group().split(" ");
      taskBuf.put(mixStr[1],Long.valueOf(mixStr[3].substring(0,mixStr[3].length()-2)));
      taskBuf.put(mixStr[4],Long.valueOf(mixStr[6]));
    }else {
      LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: bufstart = \\d+; bufvoid.*$ pattern in string");
    }
    return taskBuf;
  }

  public static String taskSoftLimit(String str){
    Matcher match =  taskSoftLimit.matcher(str);
    if(match.find()){
      String[] mixStr = match.group().split(" ");
      return mixStr[mixStr.length - 1];
    }
    LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: soft limit at.*$ pattern in string");
    return null;
  }

  public static String taskSortMB(String str){
    Matcher match =  taskSortMB.matcher(str);
    if(match.find()){
      String[] mixStr = match.group().split(" ");
      return mixStr[mixStr.length - 1];
    }
    LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: mapreduce.task.io.sort.mb:.*$ pattern in string");
    return null;
  }

  public static Integer numReduceTasks(String str){
    Matcher match =  numReduceTasks.matcher(str);
    if(match.find()){
      String[] mixStr = match.group().split(" ");
      return Integer.parseInt(mixStr[mixStr.length - 1]);
    }
    LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: numReduceTasks:.*$ pattern in string");
    return null;
  }

  public static String kvi(String str){
    Matcher match =  kvi.matcher(str);
    if(match.find()){
      String[] mixStr = match.group().split(" ");
      return mixStr[mixStr.length - 1];
    }
    LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: (EQUATOR).*$ pattern in string");
    return null;
  }

  public static String splitPath(String str){
    Matcher match =  mapTaskInput.matcher(str);
    if(match.find()){
      Matcher innerMatch = splitPath.matcher(match.group());
      if(innerMatch.find()){
        String[] mixStr = innerMatch.group().split(":");
        return mixStr[1];
      }else{

        LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: Processing split: Paths:(/\\w)+:\\d+\\+\\d+,(/\\w)+:\\d+\\+\\d+ pattern in string");
      }
    }else{
      LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: Processing split: Paths.* pattern in string");
    }
    return null;
  }

  public static String  inputFormat(String str){
    Matcher match =  mapTaskInput.matcher(str);
    if(match.find()){
      Matcher innerMatch = inputFormat.matcher(match.group());
      if(innerMatch.find()){
        String[] mixStr = innerMatch.group().split(":");
        return mixStr[1];
      }else{

        LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: Processing split: InputFormatClass:.*$ pattern in string");
      }
    }else{
      LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: Processing split: Paths.* pattern in string");
    }
    return null;
  }


}