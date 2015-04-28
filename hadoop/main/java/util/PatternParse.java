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
  private static Pattern appPattern = Pattern.compile("application_\\d+_\\d+");
  private static Pattern jobID = Pattern.compile("_\\d+_\\d+");

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
}
