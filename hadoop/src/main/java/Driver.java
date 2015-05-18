import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import parse.LogInput;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by zhangyun on 4/24/15.
 */

public class Driver {
  private static Log LOG = LogFactory.getLog(Driver.class);

  public static void main(String[] args ){

    try{
      LAConf conf = LAConf.getConf();
      LogAnalyzer LA = new LogAnalyzer(conf);
      LA.analyze();
    }catch (ParseException p){
      p.printStackTrace();
    }catch (IOException e){
      e.printStackTrace();
    }catch( Exception e){
      e.printStackTrace();
    }
  }
}
