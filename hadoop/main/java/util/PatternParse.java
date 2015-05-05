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
  private static Pattern taskID = Pattern.compile("org\\.apache\\.hadoop\\." +
          "mapred\\.Task: 'attempt_(\\d+_\\d+_[m|r]_\\d+)_\\d+' Task done\\.$");
  /**Map task log information */
  private static Pattern mapTaskInput = Pattern.compile("org\\.apache\\.hadoop" +
          "\\.mapred\\.MapTask:\\s+Processing\\s+split:\\s+Paths.*$");
  private static Pattern splitPath =
          Pattern.compile("Paths:(/\\w)+:\\d+\\+\\d+,(/\\w)+:\\d+\\+\\d+");
  private static Pattern inputFormat =
          Pattern.compile("InputFormatClass:.*$");
  private static Pattern mapOps =
          Pattern.compile("org\\.apache\\.hadoop\\.hive\\.ql\\.exec\\.mr\\.ExecMapper:$");
  private static Pattern reduceOps =
          Pattern.compile(".*ExecReducer:$");
  private static Pattern operator =
          Pattern.compile("<(\\w+)>Id =(\\d+)\\s.*$");
  private static Pattern endOperator =
          Pattern.compile("<Parent>.*</Parent>\\s.*$|</\\w+>\\s.*$");
  private static Pattern opProcFinish = Pattern.compile("org\\.apache\\.hadoop\\." +
          "hive\\.ql\\.exec\\.(\\w+): (\\d+) finished\\. closing.*$");
  private static Pattern opProcRows = Pattern.compile("org\\.apache\\.hadoop\\." +
          "hive\\.ql\\.exec\\.(\\w+): (\\d+) forwarded (\\d+) rows.*$");

  public static String jobAndID( String path ){
    Matcher match = jobPattern.matcher(path);
    if(match.find()){
      LOG.debug("Matched job_[n]_[n] pattern in path name of file");
      return match.group();
    }
    return null;
  }

  public static String jobID( String path ){
    Matcher match = jobID.matcher(path);
    if(match.find()){
      LOG.debug("Matched _[n]_[n] pattern in path name of file");
      return match.group();
    }
    return null;
  }

  public static String taskID( String path ){
    Matcher match = taskID.matcher(path);
    if(match.find()){
      LOG.debug("Matched \\d+_\\d+_[m|r]_\\d+ pattern in path name of file");
      return match.group(1);
    }
    return null;
  }

  public static String mapOps(String str){
    Matcher match = mapOps.matcher(str);
    if(match.find()){
      LOG.debug("Matched org.apache.hadoop.hive.ql.exec.mr.ExecMapper:$ " +
              "pattern in string");
      return match.group();
    }
    return null;
  }

  public static String reduceOps(String str){
    Matcher match = reduceOps.matcher(str);
    if(match.find()){
      LOG.debug("Matched.*ExecReducer:$ pattern in string");
      return match.group();
    }
    return null;
  }

  public static Tuple<String,String> operator(String str){
    Matcher match = operator.matcher(str);
    if(match.find()){
      LOG.debug("Matched <(\\w+)>Id =(\\d+)\\s*$ pattern in string");
      return new Tuple<String,String>(match.group(1),match.group(2));
    }
    return null;
  }

  public static String endOperator(String str){
    Matcher match = endOperator.matcher(str);
    if(match.find()){
      LOG.debug("Matched </\\w+>\\s*$ pattern in string");
      return match.group();
    }
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

  public static String inputFormat(String str){
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

  public static Matcher opProcFinish(String str){
    Matcher match = opProcFinish.matcher(str);
    if(match.find()){
      LOG.debug("org.apache.hadoop.hive.ql.exec.(\\w+): (\\d+) finished." +
              " closing.*$");
      return match;
    }
    return null;
  }

  public static Matcher opProcRows(String str){
    Matcher match = opProcRows.matcher(str);
    if(match.find()){
      LOG.debug("org.apache.hadoop.hive.ql.exec.(\\w+): " +
              "(\\d+) forwarded (\\d+) rows.*$");
      return match;
    }
    return null;
  }

}