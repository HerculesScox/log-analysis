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
  public List<Query> getAll() {
    List<Query> qlist = new ArrayList<Query>();
    String sql =
                " select queryString, j.workflowID, jobDependency,launchtime," +
                " username, count(j.workflowID) as jobCount" +
                "  from `table_query` q join `table_job` j " +
                " on q.workflowID = j.workflowID" +
                " group by j.workflowID";
    try {
      ResultSet res = DBHander.select(sql);
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
   * Get all job information of the query by workflowID
   * @param workflowID
   * @return
   */
  public Query getByWorkflowID(String workflowID){
    String sql = "select `queryString`,`jobid`, `workflowNode`, `detailInfo`," +
            " `jobDependency` from `log_analysis`.`table_query` q join " +
            " `log_analysis`.`table_job` j on j.workflowID = q.workflowID";
    try {
        ResultSet res = DBHander.select(sql);
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
      ResultSet res = DBHander.select(sql);
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
