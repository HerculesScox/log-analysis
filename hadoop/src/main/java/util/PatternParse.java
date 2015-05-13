package util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhangyun on 4/24/15.
 */
public class PatternParse {
  private static Log LOG = LogFactory.getLog(PatternParse.class);

  private static Pattern jobPattern = Pattern.compile("job_\\d+_\\d+");
  private static Pattern jobID = Pattern.compile("_\\d+_\\d+");

  private static Pattern taskID = Pattern.compile("org\\.apache\\.hadoop" +
          "\\.mapred\\.Task: Task:attempt_(\\d+_\\d+_[m|r]_\\d+)_\\d+ is done\\.");

  /** reduce task log information */
  private static Pattern reduceTaskInput = Pattern.compile("org\\.apache\\.hadoop" +
          "\\.mapreduce\\.task\\.reduce\\.Fetcher: for url=\\d+/mapOutput\\?job=.+&map=" +
          "(.+) sent hash and received reply$");

  /**Map task log information */
  private static Pattern mapTaskInput = Pattern.compile("org\\.apache\\.hadoop" +
          "\\.mapred\\.MapTask:\\s+Processing\\s+split:\\s+Paths:(.*)InputFormatClass: " +
          "(.*)$");

  private static Pattern mapOps =
          Pattern.compile("org\\.apache\\.hadoop\\.hive\\.ql\\.exec\\.mr\\.ExecMapper:\\s*$");
  private static Pattern reduceOps =
          Pattern.compile(".*ExecReducer:\\s*$");
  private static Pattern operator =
          Pattern.compile("<(\\w+)>Id =(\\d+)\\s*$");
  private static Pattern FSpath =
          Pattern.compile("FileSinkOperator: New Fianl Path: FS (.*)$");
  private static Pattern endOperator =
          Pattern.compile(
                  "(\\s*<Parent>.*<\\\\Parent>\\s*$)" +
                  "|" +
                  "(\\s*<\\\\w+>\\s*$)"
          );

  private static Pattern opProcFinish = Pattern.compile("org\\.apache\\.hadoop\\." +
          "hive\\.ql\\.exec\\.\\w+: \\d+ finished\\. closing.*$");
  private static Pattern opProcRows = Pattern.compile("org\\.apache\\.hadoop" +
          "\\.hive\\.ql\\.exec\\.(\\w+): (\\d+) forwarded (\\d+) rows$");
  private static Pattern opForwarding = Pattern.compile(	"(^.*) INFO \\[main\\] org" +
          "\\.apache\\.hadoop\\.hive\\.ql\\.exec\\.(\\w+): \\d+ forwarding \\d+ rows");


  public static String jobAndID( String str ){
    Matcher match = jobPattern.matcher(str);
    if(match.find()){
      LOG.debug("Matched job_[n]_[n] pattern in path name of file");
      return match.group();
    }
    return null;
  }

  public static String jobID( String str ){
    Matcher match = jobID.matcher(str);
    if(match.find()){
      LOG.debug("Matched _[n]_[n] pattern in path name of file");
      return match.group();
    }
    return null;
  }

  public static String taskID( String str ){
    Matcher match = taskID.matcher(str);
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

  /**
   * Just used reduce task log
   * @param str
   * @return
   */
  public static String FSpath(String str){
    Matcher match = FSpath.matcher(str);
    if(match.find()){
      LOG.debug("Matched FileSinkOperator: New Fianl Path: FS (.*)$pattern in string");
      return match.group(1);
    }
    return null;
  }

  public static boolean endOperator(String str){
    Matcher match = endOperator.matcher(str);
    if(match.find()){
      LOG.debug("Matched </\\w+>\\s*$ pattern in string");
      return true;
    }
    return false;
  }

  public static Matcher mapTaskInput(String str){
    Matcher match =  mapTaskInput.matcher(str);
    if(match.find()){
      LOG.debug("Can not match org.apache.hadoop.mapred.MapTask: Processing split: Paths.* pattern in string");
      return match;
    }
    return null;
  }

  public static String reduceTaskInput(String str){
    Matcher match =  reduceTaskInput.matcher(str);
    if(match.find()){
      LOG.debug("Can not match org.apache.hadoop.mapred.MapTask:or url=\\d+/mapOutput\\?job=.+&map=" +
              "(.+) sent hash and received reply$ pattern in string");
      return match.group(1);
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

  public static Matcher opForwarding(String str){
    Matcher match = opForwarding.matcher(str);
    if(match.find()){
      LOG.debug("org.apache.hadoop.hive.ql.exec.(\\w+): " +
              "\\d+ forwarding\\d+ rows.*$");
      return match;
    }
    return null;
  }

}