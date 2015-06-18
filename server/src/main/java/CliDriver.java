/**
 * Created by zhangyun on 5/15/15.
 */

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.util.*;

import bean.Job;
import bean.Query;
import bean.Task;
import com.google.gson.*;
import com.google.gson.internal.StringMap;
import com.google.gson.reflect.TypeToken;
import dao.JobDAO;
import dao.QueryDAO;
import dao.TaskDAO;
import org.json.simple.JSONValue;
import spark.ModelAndView;
import spark.Spark;
import util.Graphviz;


import static spark.Spark.get;
import static spark.Spark.post;

public class CliDriver {

  public static void main(String args[]) {
    Spark.staticFileLocation("/public");
    get("/", (request, response) -> {
      QueryDAO dao = new QueryDAO();
      List<Query> list = dao.getAll();
      List<Map<String, Object>> allInfo = new ArrayList<>();
      int i = 1;
      for (Query l : list ){
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("number", i++);
        Map<String, String> stageToJobID = dao.getWorkflowNodeByID(l.getWorkflowID());
        query.put("workflowID", l.getWorkflowID());
        query.put("username", l.getUsername());
        query.put("launchTime", l.getLaunchtime());
        query.put("queryString" , l.getQueryStirng());
        query.put("jobDependency", l.getJobDependency());
        JsonElement jelement = new JsonParser().parse(l.getJobDependency());
        Gson gson = new Gson();
        Map<String, String> json = gson.fromJson(jelement, new TypeToken<Map>(){}.getType());
        query.put("stageToJobID", stageToJobID);
        query.put("svg", Graphviz.conv(json, stageToJobID.keySet()));
        query.put("jobAmount", l.getJobAmount());
        allInfo.add(query);
      }
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Querys List");
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
      JsonElement jelement = new JsonParser().parse(job.getDetailInfo());
      Gson gson = new Gson();
      Map<String, Object> json = gson.fromJson(jelement, new TypeToken<Map>(){}.getType());
      StringMap treeMapOps = ((StringMap)json.get("mapOps"));
      LinkedHashMap mapOps = new LinkedHashMap();
      for(Object op : treeMapOps.keySet()){
        mapOps.put(op , treeMapOps.get(op));
      }
      attributes.put("mapOps", mapOps);
      attributes.put("json",json );
      return new ModelAndView(attributes, "job_detail.ftl");
    }, new FreeMarkerEngine());

    get("/jobs/:workflowID", (request, response) -> {
      String workflowID = request.params(":workflowID");
      JobDAO dao = new JobDAO();
      List<Job> jobs = dao.getByWfID(workflowID);
      List<Map<String, Object>> allInfo = new ArrayList<>();
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Jobs List");
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
      attributes.put("subTitle", "Tasks List");
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

    get("/job/*/ops/*", (request, response) -> {
      String jobid = request.splat()[0];
      String type = request.splat()[1];
      TaskDAO dao = new TaskDAO();
      List<Task> tasks = dao.getByJobid(jobid);
      List<Map<String, Object>> allInfo = new ArrayList<>();
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", type + " Operator Processing Information");
      int i = 1;
      for(Task task : tasks) {
        Map<String, Object> content = new HashMap<>();
        if(task.getTaskType().equals(type)) {
          content.put("number", i++);
          content.put("taskid", task.getTaskid());
          JsonElement jelement = new JsonParser().parse(task.getDetailInfo());
          Gson gson = new Gson();
          Map json = gson.fromJson(jelement, new TypeToken<Map>(){}.getType());
          StringMap treeMapOps = ((StringMap)json.get("operatorTree"));
          LinkedHashMap ops = new LinkedHashMap();
          for(Object op : treeMapOps.keySet()){
            ops.put(op , treeMapOps.get(op));
          }
          content.put("operatorTree", ops);
          allInfo.add(content);
        }
      }
      attributes.put("allInfo", allInfo);
      return new ModelAndView(attributes, "ops_outline.ftl");
    }, new FreeMarkerEngine());

    get("/task/:taskid", (request, response) -> {
      String taskid = request.params(":taskid");
      TaskDAO dao = new TaskDAO();
      Task task = dao.getByTaskid(taskid);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Task Detail Information");
      attributes.put("taskid", task.getTaskid());
      attributes.put("type", task.getTaskType());
      JsonElement jelement = new JsonParser().parse(task.getDetailInfo());
      Gson gson = new Gson();
      LinkedHashMap<String, Object> json = gson.fromJson(jelement, new TypeToken<LinkedHashMap>(){}.getType());
      attributes.put("attemptTasks",json.get("attemptTasks"));
      if ( json.containsKey("inputFormat")) {
        attributes.put("inputFormat", json.get("inputFormat"));
      }
      if ( json.containsKey("splitFiles")) {
        attributes.put("splitFiles",
                Arrays.asList(json.get("splitFiles").toString().split(",")));
      }
      if ( json.containsKey("inputMapTasks")) {
        System.out.println(taskid + ">>>>>>>>>>>>>> " +  Arrays.asList(json.get("inputMapTasks").toString().split(",")));
        attributes.put("inputMapTasks",
                Arrays.asList(json.get("inputMapTasks").toString().split(",")));
      }
      StringMap treeMapOps = ((StringMap)json.get("operatorTree"));
      LinkedHashMap ops = new LinkedHashMap();
      for(Object op : treeMapOps.keySet()){
        ops.put(op , treeMapOps.get(op));
      }
      attributes.put("operatorTree", ops);

      attributes.put("startTime", json.get("startTime"));
      attributes.put("error", json.get("error"));
      attributes.put("status", json.get("status"));
      attributes.put("finishTime", json.get("finishTime"));
      attributes.put("splitLocation", json.get("splitLocation"));
      attributes.put("Counter", json.get("Counter"));
      return new ModelAndView(attributes, "task_detail.ftl");
    }, new FreeMarkerEngine());

    get("/jobchart/*", (request, response) -> {
      String jobid = request.splat()[0];
      TaskDAO dao = new TaskDAO();
      List<Task> tasks = dao.getByJobid(jobid);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle","Task Chart");
      List list = new ArrayList();
      DecimalFormat df = new DecimalFormat();
      for(Task task : tasks) {
        Map<String, Object> content = new HashMap<>();
        JsonElement jelement = new JsonParser().parse(task.getDetailInfo());
        Gson gson = new Gson();
        LinkedHashMap<String, Double> json = gson.fromJson(jelement, new TypeToken<LinkedHashMap>(){}.getType());
        content.put("taskid", task.getTaskid());
        content.put("tasktype",task.getTaskType());
        content.put("startTime",BigDecimal.valueOf(json.get("startTime")).longValue());
        content.put("finishTime",BigDecimal.valueOf(json.get("finishTime")).longValue());
        list.add(content);
        if(task.getTaskType().equals("REDUCE")) {
          System.out.println("------------------------");
          System.out.println(task.getTaskid());
          System.out.println("startTime :" + BigDecimal.valueOf(json.get("startTime")).longValue());
          System.out.println("finishTime :" + BigDecimal.valueOf(json.get("finishTime")).longValue());
        }
      }
      String data = JSONValue.toJSONString(list);
      attributes.put("data", data);
      return new ModelAndView(attributes, "job_chart.ftl");
    }, new FreeMarkerEngine());

    post("/jobchart/*/tasktype/*", (request, response) -> {
      String jobid = request.splat()[0];
      String type = request.splat()[1];
      TaskDAO dao = new TaskDAO();
      List<Task> tasks = dao.getByJobid(jobid);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Task Chart");
      List list = new ArrayList();
      for (Task task : tasks) {
        Map<String, Object> content = new HashMap<>();
        if (task.getTaskType().equals(type)) {
          JsonElement jelement = new JsonParser().parse(task.getDetailInfo());
          Gson gson = new Gson();
          Map json = gson.fromJson(jelement, new TypeToken<Map>() {
          }.getType());
          content.put("taskid", task.getTaskid());
          content.put("tasktype", task.getTaskType());
          content.put("startTime", json.get("startTime"));
          content.put("finishTime", json.get("finishTime"));
          list.add(content);
        }
      }
      String data = JSONValue.toJSONString(list);
      System.out.println("json :" + data);
      attributes.put("data", list);
      return new ModelAndView(attributes, "job_chart.ftl");
    }, new FreeMarkerEngine());
  }

}