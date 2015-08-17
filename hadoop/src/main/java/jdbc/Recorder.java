package jdbc;

import base.Query;
import base.Task;
import base.Job;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by zhangyun on 5/14/15.
 */
public class Recorder {
  private static Log LOG = LogFactory.getLog(Recorder.class);

  /**
   * Record task information used insert sql
   * @param task
   * @param Jobid
   * @return
   * @throws IOException
   */
  public static boolean taskInfoRecord( Task task , String Jobid ) throws IOException{
    LinkedList<String> params = new LinkedList<String>();
    String sql = "INSERT INTO `log_analysis`.`table_task` " +
            "(`taskid`, `jobid`, `taskType`, `logPath`, `detailInfo`) " +
            "VALUES ( ?, ?, ?, ?, ?)";
    params.add(task.getTaskID());
    params.add(Jobid);
    params.add(task.getTaskInfo().getTaskType().toString());
    params.add(task.getTaskLogPath().toString());
    params.add(task.toJSON());
    int res = DBHander.execQuery(sql, params);
    if ( res > 0 ){
      LOG.info(" insert task " + task.getTaskID() + " information into" +
              " database successfully");
      return true;
    }
    return false;
  }

  /**
   * Record job information used insert sql
   * @param job
   * @return
   * @throws IOException
   */
  public static boolean jobInfoRecord( Job job ) throws IOException {
    LinkedList<String> params = new LinkedList<String>();
    String sql = "INSERT INTO `log_analysis`.`table_job` " +
            "(`jobid`, `workflowID`, `workflowNode`, `logPath`, `detailInfo`) " +
            "VALUES ( ?, ?, ?, ?, ?)";
    params.add(job.getJobInfo().getJobid().toString());
    params.add(job.getJobInfo().getWorkflowId());
    params.add(job.getJobInfo().getWorkflowNodeName());
    params.add(job.getJHPath().toString());
    params.add(job.toJSON());
    int res = DBHander.execQuery(sql, params);
    if ( res > 0 ){
      System.out.println("> job record successfully");
      LOG.info(" insert job " + job.getJobInfo().getJobid() + " information into" +
              " database successfully");
      return true;
    }
    return  false;
  }
  /**
   * insert Query entity information into database
   * @param query
   * @return
   */
  public static boolean queryInfoRecord( Query query ) throws IOException{
    LinkedList<String> params = new LinkedList<String>();
    String sql = "INSERT INTO `log_analysis`.`table_query` " +
            "(`queryString`, `workflowID`, `jobDependency`, `username`, `launchtime`) " +
            "VALUES ( ?, ?, ?, ?,?)";
    params.add(StringEscapeUtils.escapeSql(query.getQueryString()));
    params.add(query.getWorkflowID());
    params.add(query.parseStages());
    params.add(query.getJobs().get(0).getJobInfo().getUsername());
    params.add(String.valueOf(query.getLaunchTime()));
    int res = DBHander.execQuery(sql, params);
    if ( res > 0 ){
      System.out.println("> query record successfully");
      LOG.info(" insert query " + query.getWorkflowID() + " information into" +
              " database successfully");
      return true;
    }
    System.out.println("> query record failed!");
    return false;
  }
}
