package dao;

import bean.Query;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyun on 5/18/15.
 */
public class QueryDAO {
  private static Log LOG = LogFactory.getLog(QueryDAO.class);

 /**
  * Get all query information
  * @return
  */
  public List<Query> getAll(int page, int maxSize, String user) {
    List<Query> qlist = new ArrayList<Query>();
    String sql =
            " select queryString, j.workflowID, jobDependency,launchtime," +
                    " username, count(j.workflowID) as jobCount" +
                    " from `table_query` q join `table_job` j " +
                    " on q.workflowID = j.workflowID " ;
    if(!user.equals("all") && user != null){
        sql += "where q.username='" + user + "'";
    }
    sql += " group by j.workflowID order by launchtime desc;";
    try {
      int startRow = (page - 1) * maxSize;
      int endRow = startRow + maxSize + 1;
      ResultSet res = DBHander.select(sql,page, endRow);
      if( startRow != 0)
        res.absolute(startRow);
      else
        res.beforeFirst();
      while (res.next()) {
        String queryString = res.getString("queryString");
        String workflowID = res.getString("workflowID");
        String jobDependency = res.getString("jobDependency");
        String username = res.getString("username");
        Integer jobAmount = res.getInt("jobCount");
        Long launchtime = res.getLong("launchtime");
        Query query = new Query(queryString, workflowID, jobDependency,
                username, jobAmount, launchtime);
        qlist.add(query);
      }
      res.close();
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
    return qlist;
  }

  /**
   * Get query number
   * @return
   */
  public int getQueryNums( String user){
    int nums = -1;
    String sql = "select count(*) as nums from `log_analysis`.`table_query` ";
    if(!user.equals("all") && user != null){
      sql += "where username='" + user + "'";
    }
    try {
      ResultSet res = DBHander.select(sql,-1,-1);
      res.next();
      nums = res.getInt("nums");
      res.close();
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
    return nums;
  }

  /**
   * Get all job information of the query by workflowID
   * @param workflowID
   * @return
   */
  public Query getByWorkflowID(String workflowID){
    String sql = "select `queryString`,`jobid`, `workflowNode`, `detailInfo`," +
            " `jobDependency` from `log_analysis`.`table_query` q join " +
            " `log_analysis`.`table_job` j on j.workflowID = q.workflowID";
    try {
        ResultSet res = DBHander.select(sql,-1,-1);
        while(res.next()){
          String jobid = res.getString("jobid");
          String workflowNode = res.getString("workflowNode");
          String detailInfo = res.getString("detailInfo");
          String jobDependency = res.getString("jobDependency");
          String queryString = res.getString("queryString");
        }
        res.close();
    }catch (SQLException e){
        LOG.debug(e.getMessage());
        e.printStackTrace();
    }
    return null;
  }


  public Map<String,String> getWorkflowNodeByID(String workflowID){
    Map<String,String> list = new HashMap<>();
    String sql = "SELECT `jobid`, `workflowNode` " +
            "FROM `table_job` " +
            "WHERE workflowID='" + workflowID+"'";
    try {
      ResultSet res = DBHander.select(sql,-1,-1);
      while(res.next()){
        String jobid = res.getString("jobid");
        String workflowNode = res.getString("workflowNode");
        list.put(workflowNode,jobid);
      }
      res.close();
      return list;
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
    return null;
  }
}
