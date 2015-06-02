package dao;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.LinkedList;


/**
 * Created by zhangyun on 5/14/15.
 */
public class DBHander {
  private static Log LOG = LogFactory.getLog(DBHander.class);
  private final static String host = "jdbc:mysql://localhost:3306/log_analysis";
  private final static String username = "root";
  private final static String password = "123456";

  private static Connection con = dbConnect();


  /**
   * Create a single instance of Connection
   * @return Connection instance
   */
  private static Connection dbConnect() {
    if( con == null) {
      try {
        Class.forName("com.mysql.jdbc.Driver");
        con = DriverManager.getConnection(host, username, password);
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

  public static ResultSet select(String sql){
    try{
      Statement statement = con.createStatement();
      return statement.executeQuery(sql);
    }catch ( SQLException e ){
      System.out.println(e.getMessage());
      LOG.debug("SQL executed failed!");
    }
    return null;
  }

  /**
   * execute dynamic DDL used
   * @param sql
   * @param params
   * @return
   */
  public static ResultSet select(String sql , LinkedList<String> params ){
    try{
      int i = 1;
      PreparedStatement ps = con.prepareStatement(sql);
      if( params != null ){
        for(String p : params){
          ps.setString(i++, p);
        }
      }
      return ps.executeQuery();
    }catch ( SQLException e ){
      System.out.println(e.getMessage());
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
