package dao;

import bean.Job;
import bean.Query;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangyun on 5/21/15.
 */
public class JobDAO {
  private static Log LOG = LogFactory.getLog(QueryDAO.class);

  /**
   * Get  job information by jobid
   * @param jobid
   * @return
   */
  public Job getByJobid(String jobid){
    Job job = null;
    String sql =
            "select `workflowID`, `workflowNode`, `logPath`, `detailInfo`" +
            " from `log_analysis`.`table_job` " +
            " where jobid='" + jobid + "'";
    try {
      ResultSet res = DBHander.select(sql,-1,-1);
      while (res.next()) {
        String logPath = res.getString("logPath");
        String workflowID = res.getString("workflowID");
        String workflowNode = res.getString("workflowNode");
        String detailInfo = res.getString("detailInfo");
        job = new Job(jobid, workflowID, workflowNode, logPath, detailInfo);
      }
      res.close();
      return job;
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Get  job information by workflowID
   * @param workflowID
   * @return
   */
  public  List<Job> getByWfID(String workflowID){
    List<Job> jobs = new ArrayList<>();
    String sql =
            "select j.jobid, `workflowNode`, j.logPath, j.detailInfo, count(taskid) as taskNum" +
                    " from table_job j join table_task t " +
                    " on j.jobid=t.jobid" +
                    " where workflowID='" + workflowID + "'" +
                    " group by j.jobid";
    try {
      ResultSet res = DBHander.select(sql,-1,-1);
      while (res.next()) {
        String logPath = res.getString("logPath");
        String jobid = res.getString("jobid");
        String workflowNode = res.getString("workflowNode");
        String detailInfo = res.getString("detailInfo");
        int taskNum = res.getInt("taskNum");
        Job job = new Job(jobid, workflowID, workflowNode, logPath, detailInfo ,taskNum);
        jobs.add(job);
      }
      res.close();
      return jobs;
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

}
