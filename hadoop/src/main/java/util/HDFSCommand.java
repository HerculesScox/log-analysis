package util;

import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;

/**
 * Created by zhangyun on 4/22/15.
 */
public class HDFSCommand {
  private static Log LOG = LogFactory.getLog(HDFSCommand.class);

  public static HashSet<Path> list(String path , LAConf conf)throws IOException{
    return list(path , null, conf);
  }

  public static HashSet<Path> list(String path , String regex ,
                                      LAConf conf)throws IOException{

    HashSet<Path> paths = new HashSet<Path>();
    FileSystem fs = FileSystem.get(URI.create(path) , conf);
    RemoteIterator<LocatedFileStatus> listFiles =  fs.listFiles(new Path(path), true);

    while(listFiles.hasNext()){
      LocatedFileStatus status = listFiles.next();
      if(!status.isDirectory()){
        if(regex != null && status.getPath().toString().endsWith(regex)) {
          paths.add(status.getPath());
        }
        if(regex == null){
          paths.add(status.getPath());
        }
      }
    }
    return paths;
  }

  private static PathFilter regexPath( final String regex ){
    return new PathFilter( ) {
      @Override
      public boolean accept(Path path) {
        return path.toString().endsWith(regex);
      }
    };
  }

  public static void delete(String path , LAConf conf) throws IOException{
    FileSystem fs = FileSystem.get(URI.create(path), conf);
    Path p = new Path(path);
    if( fs.exists(p)){
      fs.delete(p , true);
      LOG.info(p.getName() + " has been deleted!");
    }else{
      LOG.info(p.getName() + " is non-exist!");
    }
  }

  /**
   * After analyzing all log files. move them into backup directory
   * @param conf
   * @throws IOException
   */
  public static void clearAndBackup(LAConf conf) {
    Path taskLogs= new Path(LAConf.getYarnLogPath());
    Path jobLogs = new Path(LAConf.getJHLogPath());
    try {
      FileSystem fs = FileSystem.get(URI.create(LAConf.getDfs() + "/"), conf);

      if (!fs.exists(taskLogs) || !fs.exists(jobLogs)) {
        //The directory non-exists, handle failed!
        return;
      }

      DateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");
      Date currentDate = Calendar.getInstance().getTime();

      Path backupDir = new Path(LAConf.getBackupDir());
      if (!fs.exists(backupDir)) {
        System.out.println("Make dir " + backupDir);
        fs.mkdirs(backupDir);
      }

      Path datePath = new Path(LAConf.getBackupDir() + "/" + fmtDate.format(currentDate));
      if (!fs.exists(datePath)) {
        System.out.println("Make dir " + datePath);
        fs.mkdirs(datePath);
      }

      Path nodeLogPath = new Path(datePath.toString() + "/" + "node_log_dir" );
      Path jobLogPath = new Path(datePath.toString() + "/" + "jobhistory_log_dir" );
      if( !fs.exists(nodeLogPath)){
        fs.mkdirs(nodeLogPath);
      }
      if( !fs.exists(jobLogPath)){
        fs.mkdirs(jobLogPath);
      }

      DateFormat fmtTime = new SimpleDateFormat("HH_mm_ss");
      String indx = fmtTime.format(Calendar.getInstance().getTime());
      Path tdestPath = new Path(nodeLogPath.toString() + "/" + indx);
      Path jdestPath = new Path(jobLogPath.toString() + "/" + indx);
      if( !fs.exists(tdestPath)){
        fs.mkdirs(tdestPath);
      }
      if( !fs.exists(jdestPath)){
        fs.mkdirs(jdestPath);
      }

      if (!fs.rename(taskLogs, tdestPath)) {
        System.out.println("Backup from "+ taskLogs + " to " + tdestPath + " directory failed! " );
      }

      if (!fs.rename(jobLogs, jdestPath)) {
        System.out.println("Backup from "+ jobLogs + " to " + jdestPath + " directory failed! "  );
      }

    }catch (IOException e){
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }
}
