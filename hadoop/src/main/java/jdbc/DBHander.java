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
   * execute dynamic sql
   * @param sql
   * @param params
   * @return
   */
  public static ResultSet execQuery(String sql , LinkedList<Object> params ){
      try{
        int i = 1;
        PreparedStatement ps = con.prepareStatement(sql);
        if( params != null ){
           for(Object p : params){
            ps.setObject(i++, p);
           }
        }
        return ps.executeQuery();
      }catch ( SQLException e ){
        LOG.debug("SQL executed failed!");
      }
      return null;
  }

  /**
   * execute static sql
   * @param sql
   * @return
   */
  public static ResultSet execQuery(String sql){
    try{

      Statement statement = con.createStatement();
      return statement.executeQuery(sql);
    }catch ( SQLException e ){
      LOG.debug("SQL executed failed!");
    }
    return null;
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
