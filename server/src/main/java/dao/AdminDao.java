package dao;

import bean.Job;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zhangyun on 7/7/15.
 */
public class AdminDao {
  private static Log LOG = LogFactory.getLog(QueryDAO.class);

  public boolean verification(String username ,String password){
    boolean result = false;
    String sql =
            "SELECT * FROM `table_admin` " +
            "WHERE `username` = '" + username +"'"+
            "AND `password` = '"+ password + "'";
    try {
      ResultSet res = DBHander.select(sql,-1,-1);
      if (res.next()) result = true;
      res.close();
      return result;
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
    return result;
  }

}
