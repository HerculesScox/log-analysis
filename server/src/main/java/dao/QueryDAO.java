package dao;

import jdbc.DBHander;
import bean.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangyun on 5/18/15.
 */
public class QueryDAO {

  public List<Query> getAll() throws SQLException{
    List<Query> qlist = new ArrayList<Query>();
    String sql = "select `queryString`, `workflowID`, `jobDependency`," +
            " `username` from `log_analysis`.`table_query`";
    ResultSet res = DBHander.select(sql);
    while(res.next()){
      String queryString = res.getString("queryString");
      String workflowID = res.getString("workflowID");
      String jobDependency = res.getString("jobDependency");
      String username = res.getString("username");
      Query query = new Query(queryString, workflowID, jobDependency, username);
      qlist.add(query);
    }
    res.close();
    return qlist;
  }

  public Query getByWorkflowID(String workflowID) throws SQLException{

    String sql = "select `queryString`,`jobid`, `workflowNode`, `detailInfo`," +
            " `jobDependency` from `log_analysis`.`table_query` q join " +
            " `log_analysis`.`table_job` j on j.workflowID = q.workflowID";
    ResultSet res = DBHander.select(sql);
    while(res.next()){
      String jobid = res.getString("jobid");
      String workflowNode = res.getString("workflowNode");
      String detailInfo = res.getString("detailInfo");
      String jobDependency = res.getString("jobDependency");
      String queryString = res.getString("queryString");

    }
    res.close();
    return null;
  }

}
