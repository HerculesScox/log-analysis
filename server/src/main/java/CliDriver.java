/**
 * Created by zhangyun on 5/15/15.
 */

import java.io.StringWriter;
import java.util.*;

import bean.Job;
import bean.Query;
import bean.Task;
import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import dao.JobDAO;
import dao.QueryDAO;
import dao.TaskDAO;
import org.apache.commons.lang.StringEscapeUtils;
import org.json.simple.*;
import org.json.simple.JSONArray;
import spark.ModelAndView;
import spark.Spark;


import static spark.Spark.get;

public class CliDriver {

  public static void main(String args[]) {
    Spark.staticFileLocation("/public");
    get("/", (request, response) -> {
      QueryDAO dao = new QueryDAO();
      List<Query> list = dao.getAll();
      List<Map<String, Object>> allInfo = new ArrayList<>();
      int i = 1;
      for (Query l : list ){
        Map<String, Object> query = new HashMap<>();
        query.put("number", i++);
        Map<String, String> stageToJobID = dao.getWorkflowNodeByID(l.getWorkflowID());
        query.put("workflowID", l.getWorkflowID());
        query.put("username", l.getUsername());
        query.put("launchTime", l.getLaunchtime());
        query.put("queryString" , l.getQueryStirng());
        query.put("jobDependency", l.getJobDependency());
        query.put("stageToJobID", stageToJobID);
        query.put("jobAmount", l.getJobAmount());
        allInfo.add(query);
      }
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Query List");
      attributes.put("allInfo", allInfo);
      return new ModelAndView(attributes, "index.ftl");
    }, new FreeMarkerEngine());

    get("/job/:jobid", (request, response) -> {
      String jobid = request.params(":jobid");
      JobDAO dao = new JobDAO();
      Job job = dao.getByJobid(jobid);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "JOB Detail Information");
      attributes.put("jobid", job.getJobid());
      attributes.put("stage", job.getWorkflowNode());
      attributes.put("detail", job.getDetailInfo());
      return new ModelAndView(attributes, "job_detail.ftl");
    }, new FreeMarkerEngine());

    get("/jobs/:workflowID", (request, response) -> {
      String workflowID = request.params(":workflowID");
      JobDAO dao = new JobDAO();
      List<Job> jobs = dao.getByWfID(workflowID);
      List<Map<String, Object>> allInfo = new ArrayList<>();
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Job List");
      int i = 1;
      for(Job job : jobs) {
        Map<String, Object> content = new HashMap<>();
        content.put("number", i++);
        content.put("jobid", job.getJobid());
        content.put("stage", job.getWorkflowNode());
        content.put("taskNum", job.getTaskNum());
        JsonElement jelement = new JsonParser().parse(job.getDetailInfo());
        Long launchtime = jelement.getAsJsonObject().get("launchTime").getAsLong();
        content.put("launchtime", launchtime);
        content.put("detail", job.getDetailInfo());
        allInfo.add(content);
      }
      attributes.put("allInfo", allInfo);
      return new ModelAndView(attributes, "job_all.ftl");
    }, new FreeMarkerEngine());

    get("/tasks/:jobid", (request, response) -> {
      String jobid = request.params(":jobid");
      TaskDAO dao = new TaskDAO();
      List<Task> tasks = dao.getByJobid(jobid);
      List<Map<String, Object>> allInfo = new ArrayList<>();
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Task List");
      int i = 1;
      for(Task task : tasks) {
        Map<String, Object> content = new HashMap<>();
        content.put("number", i++);
        content.put("taskid", task.getTaskid());
        content.put("type", task.getTaskType());
        content.put("detail", task.getDetailInfo());
        allInfo.add(content);
      }
      attributes.put("allInfo", allInfo);
      return new ModelAndView(attributes, "task_all.ftl");
    }, new FreeMarkerEngine());

    get("/task/:taskid", (request, response) -> {
      String taskid = request.params(":taskid");
      TaskDAO dao = new TaskDAO();
      Task task = dao.getByTaskid(taskid);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Task Detail Information");
      attributes.put("taskid", task.getTaskid());
      attributes.put("type", task.getTaskType());
      System.out.println(task.getDetailInfo());
      JsonElement jelement = new JsonParser().parse(task.getDetailInfo());
      Gson gson = new Gson();
      Map<String, Object> json = gson.fromJson(jelement, new TypeToken<Map>(){}.getType());
      attributes.put("attemptTask",
        Arrays.asList(json.get("attemptTask").toString().split(",")));
      if ( json.containsKey("inputFormat")) {
        attributes.put("inputFormat", json.get("inputFormat"));
      }
      if ( json.containsKey("splitFiles")) {
        attributes.put("splitFiles",
                Arrays.asList(json.get("splitFiles").toString().split(",")));
      }
      if ( json.containsKey("inputMapTasks")) {
        attributes.put("inputMapTasks",
                Arrays.asList(json.get("inputMapTasks").toString().split(",")));
      }
      attributes.put("operatorTree", json.get("operatorTree"));
      attributes.put("Counter", json.get("Counter"));
      return new ModelAndView(attributes, "task_detail.ftl");
    }, new FreeMarkerEngine());
  }

}