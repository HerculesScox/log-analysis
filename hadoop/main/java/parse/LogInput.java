package parse;

import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import util.HDFSCommand;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by zhangyun on 4/21/15.
 * Find All log files input path in HDFS
 */
public class LogInput {
  private static final String JHIST_File = ".jhist";
  private static final String XML_FILE = ".xml";
  private LAConf conf;

  public LogInput( LAConf conf ) {
    this.conf = conf;
  }

  /**
   * generate path  list of .jhist  files for job history files
   * @return
   * @throws IOException
   */
  public HashSet<Path> genJFPath() throws IOException{
    String jPath = conf.getJHLogPath();
    String newPath =  conf.getDfs()+jPath;
    return HDFSCommand.list(newPath , JHIST_File , conf);
  }

  /**
   * generate path  list of .xml files for all job history files
   * @return
   * @throws IOException
   */
  public HashSet<Path> genXFPath() throws IOException{
    String xPath = conf.getJHLogPath();
    String newPath =  conf.getDfs()+xPath;
    return HDFSCommand.list(newPath , XML_FILE , conf);
  }

  /**
   * Get all task execution log file from after yarn aggregation log files
   * @return
   * @throws IOException
   */
  public HashSet<Path> genTFPath()throws IOException{
    String yPath = conf.getYarnLogPath();
    String newPath =  conf.getDfs()+yPath;
    return HDFSCommand.list(newPath  , conf);
  }
}
