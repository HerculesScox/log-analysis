package jdbc;

import conf.LAConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.LinkedList;


/**
 * Created by zhangyun on 5/14/15.
 */
public class DBHander {
  private static Log LOG = LogFactory.getLog(DBHander.class);
  private static Connection con = dbConnect();


  /**
   * Create a single instance of Connection
   * @return Connection instance
   */
  private static Connection dbConnect() {
    if( con == null) {
      try {
        Class.forName("com.mysql.jdbc.Driver");
        con = DriverManager.getConnection(LAConf.getDBHost(),
                LAConf.getDBUsername(), LAConf.getDBPassword());
        return  con;
      } catch (ClassNotFoundException e) {
        LOG.debug("Can not found com.mysql.jdbc.Driver class!");
        e.printStackTrace();
      } catch (SQLException e) {
        LOG.debug("Database connected failed!");
      }
    }
    return con;
  }

  /**
   * execute dynamic sql used
   * @param sql
   * @param params
   * @return
   */
  public static int execQuery(String sql , LinkedList<String> params ){
    int res = 0;
    try{
      int i = 1;
      PreparedStatement ps = con.prepareStatement(sql);
      if( params != null ){
         for(String p : params){
          ps.setString(i++, p);
         }
      }
      res = ps.executeUpdate();
      ps.close();
      return  res;
    }catch ( SQLException e ){
      System.out.println(e.getMessage());
      LOG.debug("SQL executed failed!");
    }
    return 0;
  }

  /**
   * execute static sql(DML)
   * @param sql
   * @return  if return value great than 0 ,insert manipulation is successful.
   */
  public static int execQuery(String sql){
    int res = 0;
    try{
      Statement statement = con.createStatement();
      res = statement.executeUpdate(sql);
      statement.close();
      return res;
    }catch ( SQLException e ){
      System.out.println(e.getMessage());
      LOG.debug("SQL executed failed!");
    }
    return res;
  }

  /**
   * database close manipulation
   */
  public static void close(){
     try {
       con.close();
     }catch (SQLException e){
       LOG.debug("The database closing failed!");
     }
  }

}
