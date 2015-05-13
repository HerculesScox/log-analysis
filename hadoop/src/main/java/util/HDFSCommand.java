package util;

import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;

import java.util.HashSet;

/**
 * Created by zhangyun on 4/22/15.
 */
public class HDFSCommand {
  private static Log LOG = LogFactory.getLog(HDFSCommand.class);

  public static HashSet<Path> list(String path , LAConf conf)throws IOException{
    return list(path , null, conf);
  }

  public static  HashSet<Path> list(String path , String regex ,
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

}
