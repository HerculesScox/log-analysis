package dao;

import bean.Query;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangyun on 5/18/15.
 */
public class QueryDAO {
  private static Log LOG = LogFactory.getLog(QueryDAO.class);

  public List<Query> getAll() {
    List<Query> qlist = new ArrayList<Query>();
    String sql =
                " select queryString, j.workflowID, jobDependency," +
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
        Query query = new Query(queryString, workflowID, jobDependency,
                username, jobAmount);
        qlist.add(query);
      }
      res.close();
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
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
