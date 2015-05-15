package conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringInterner;
import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.*;
import org.xml.sax.SAXException;
import util.Tuple;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;


/**
 * Created by zhangyun on 4/22/15.
 * The configuration of project, it is used to add
 * some configuration options
 */
public class LAConf extends Configuration {
  private static Log LOG = LogFactory.getLog(LAConf.class.getName());
  private static String hadoopConfPath;
  private static Properties props = prop();

  private static final String username = props.getProperty("username") ;
  private static final String dsf = props.getProperty("fs.defaultFS") ;
  private static Tuple<String,String> NMLOG = new Tuple<String,String>
          ("yarn.nodemanager.remote-app-log-dir",
                  props.getProperty("yarn.node.manager.remote-app-log-dir"));
  private static Tuple<String,String> JHLOG = new Tuple<String,String>
          ("mapreduce.jobhistory.done-dir",
                  props.getProperty("mapreduce.jobhistory.done-dir"));

  /** Preprocessing */
  static {

    for (Map.Entry<String, String> env : System.getenv().entrySet()) {
      if (env.getKey().equals("HADOOP_CONF_DIR") || env.getKey().equals("HADOOP_CONF")) {
        hadoopConfPath = env.getValue();
        break;
      }
    }
  }

  public LAConf(){
    super();
    parseConfiguration();
  }

  private final  static LAConf conf = new LAConf();

  public static LAConf getConf() throws Exception{
    if(conf != null){
      return conf;
    }else{
      throw new Exception("Configuration can not be initialized !");
    }
  }
  private static Properties prop(){
    Properties  props = new Properties();
     File[] propFiles = new File("hadoop/src/main/resources").listFiles(
             new FilenameFilter() {
       @Override
       public boolean accept(File dir, String name) {
         return name.endsWith(".properties");
       }
     });

    try {
      for (File file : propFiles) {
        if( file.getName().startsWith("log4j")){
          PropertyConfigurator.configure(file.getAbsolutePath());
          continue;
        }
        FileInputStream fis = new FileInputStream(file.getAbsoluteFile());
        props.load(fis);
        fis.close();
      }
    }catch (IOException e){
      LOG.error("The configuration could not load!");
    }
    return props;
  }

  private static void parseConfiguration( ){
    String yarnSite = hadoopConfPath + "/yarn-site.xml";
    String ylaPath = parseConfiguration( yarnSite , NMLOG.getKey());
    NMLOG.setValue(ylaPath);
    LOG.info(NMLOG.getKey() +" path :   " +NMLOG.getValue());
    String jhPath = hadoopConfPath + "/mapred-site.xml";
    String jobHistoryPath = parseConfiguration( jhPath , JHLOG.getKey());
    JHLOG.setValue(jobHistoryPath);
    LOG.info(JHLOG.getKey() +" path :    " +JHLOG.getValue());
  }

  /**
   * Parsing hadoop configuration files ,in this method is used to parse
   * yarn-site.xml and mapred-site.xml files
   * @param logFilePath
   * @param name
   * @return
   */
  private static String parseConfiguration( String logFilePath , String name){
    try {
      DocumentBuilderFactory docBuilderFactory
              = DocumentBuilderFactory.newInstance();

      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();

      File file = new File(logFilePath);
      Document doc = builder.parse(file);
      if (doc == null) {
        throw new RuntimeException("Configuration files parsing failed!");
      }
      Element root = doc.getDocumentElement();

      if (!"configuration".equals(root.getTagName()))
        LOG.fatal("bad conf file: top-level element not <configuration>");

      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);

        if (!(propNode instanceof Element)) {
          continue;
        }

        Element prop = (Element) propNode;
        if (!"property".equals(prop.getTagName()))
          LOG.warn("bad conf file: element not <property>");
        NodeList fields = prop.getChildNodes();

        boolean found = false;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
            String attr = StringInterner.weakIntern(
                    ((Text) field.getFirstChild()).getData().trim());
            if( attr.equals(name)) {
              found = true;
            }
          }
          if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
            String value = StringInterner.weakIntern(
                    ((Text) field.getFirstChild()).getData());
            if( ! value.equals("null") && found) {
              return value;
            }
          }
        }
      }
    } catch (IOException e){
      LOG.debug("Configuration files IO  error!");
      e.printStackTrace();
    } catch( ParserConfigurationException e){
      LOG.debug("Parse configuration files error!");
      e.printStackTrace();
    }catch(SAXException e){
      LOG.debug("Parse configuration files Exception!");
      e.printStackTrace();
    }
    return null;
  }

  public static String getYarnLogPath(){
    return NMLOG.getValue();
  }

  public static String getJHLogPath(){
    return JHLOG.getValue();
  }

  public static String getDfs(){
    return dsf;
  }

  public static String getSysUsername(){ return username;}

  public static String getDBUsername(){
    return  props.getProperty("dbUername").trim();
  }

  public static String getDBHost(){
    return  props.getProperty("dbHost").trim();
  }

  public static String getDBPassword(){
    return  props.getProperty("dbPassword").trim();
  }

}



