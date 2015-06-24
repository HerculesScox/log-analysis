package dao;

import bean.Job;
import bean.Task;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangyun on 5/22/15.
 */
public class TaskDAO {
  private static Log LOG = LogFactory.getLog(QueryDAO.class);
  /**
   * Get one group tasks information by jobid
   * @param jobid
   * @return
   */
  public List<Task> getByJobid(String jobid){
    List<Task> tasks = new ArrayList<>();
    String sql =
            "select`taskid`, `jobid`, `taskType`, `logPath`, `detailInfo`" +
                    " from `log_analysis`.`table_task` " +
                    " where jobid='" + jobid + "'";
    try {
      ResultSet res = DBHander.select(sql);
      while (res.next()) {
        String taskid = res.getString("taskid");
        String logPath = res.getString("logPath");
        String taskType = res.getString("taskType");
        String detailInfo = res.getString("detailInfo");
        Task task = new Task( taskid, taskType, logPath, detailInfo) ;
        tasks.add(task);
      }
      res.close();
      return tasks;
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Get task information by taskid
   * @param taskid
   * @return
   */
  public Task getByTaskid(String taskid){
    Task task = null;
    String sql =
            "select`taskid`, `jobid`, `taskType`, `logPath`, `detailInfo`" +
                    " from `log_analysis`.`table_task` " +
                    " where taskid='" + taskid + "'";
    try {
      ResultSet res = DBHander.select(sql);
      while (res.next()) {
        String jobid = res.getString("jobid");
        String logPath = res.getString("logPath");
        String taskType = res.getString("taskType");
        String detailInfo = res.getString("detailInfo");
        task = new Task( taskid, taskType, logPath, detailInfo, jobid) ;
      }
      res.close();
      return task;
    }catch (SQLException e){
      LOG.debug(e.getMessage());
      e.printStackTrace();
    }
    return null;
  }
}
