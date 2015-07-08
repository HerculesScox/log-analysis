/**
 * Created by zhangyun on 5/15/15.
 */

import java.math.BigDecimal;
import java.util.*;

import bean.Job;
import bean.Query;
import bean.Task;
import com.google.gson.*;
import com.google.gson.internal.StringMap;
import com.google.gson.reflect.TypeToken;
import dao.AdminDao;
import dao.JobDAO;
import dao.QueryDAO;
import dao.TaskDAO;
import org.json.simple.JSONValue;
import spark.ModelAndView;
import spark.Spark;
import util.GenRenderData;
import util.Graphviz;


import static spark.Spark.get;
import static spark.Spark.post;

public class CliDriver {

  public static void main(String args[]) {
    Spark.staticFileLocation("/public");
    get("/", (request, response) -> {
      Map<String, Object> attributes = new HashMap<>();
      List<Map<String, Object>> allInfo = GenRenderData.mainPage(1,attributes,"all");
      attributes.put("subTitle", "Query List");
      attributes.put("allInfo", allInfo);
      attributes.put("user", "all");
      return new ModelAndView(attributes, "index.ftl");
    }, new FreeMarkerEngine());

    get("/page/:index/user/:user", (request, response) -> {
      String page = request.params(":index");
      String user = request.params(":user");
      Map<String, Object> attributes = new HashMap<>();
      List<Map<String, Object>> allInfo =
                GenRenderData.mainPage(Integer.valueOf(page),attributes,user);
      attributes.put("subTitle", "Query List");
      attributes.put("allInfo", allInfo);
      attributes.put("user", user);
      return new ModelAndView(attributes, "index.ftl");
    }, new FreeMarkerEngine());

    post("/index_user", (request, response) -> {
      String user = request.queryParams("user");
      Map<String, Object> attributes = new HashMap<>();
      List<Map<String, Object>> allInfo =
              GenRenderData.mainPage(1, attributes, user);
      attributes.put("subTitle", "Query List");
      attributes.put("allInfo", allInfo);
      attributes.put("user", user);
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
      List<Map<String, Object>> allInfo = GenRenderData.jobsList(workflowID);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Jobs List");
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

      List redList = new ArrayList();
      //add map output to which reduce task
      if(task.getTaskType().equals("MAP")) {
        List<Task> tasks = dao.getByJobid(task.getJobid());
        for (Task tk : tasks) {
          if (tk.getTaskType().equals("REDUCE")) {
            JsonElement jelementTk = new JsonParser().parse(tk.getDetailInfo());
            LinkedHashMap<String, Object> jsonTk =
              gson.fromJson(jelementTk, new TypeToken<LinkedHashMap>() {}
                  .getType());
            String matchStr = jsonTk.get("inputMapTasks")
                                    .toString()
                                    .replace("[", "")
                                    .replace("]", "")
                                    .replace(" ","");
            List<String> inputOfRedTask =  Arrays.asList(matchStr.split(","));
            if (inputOfRedTask.contains(task.getTaskid())) {
              redList.add(tk.getTaskid());
            }
          }
        }
      }
      attributes.put("outputTo", redList);
      return new ModelAndView(attributes, "task_detail.ftl");
    }, new FreeMarkerEngine());

    get("/jobchart/*", (request, response) -> {
      String jobid = request.splat()[0];
      TaskDAO dao = new TaskDAO();
      List<Task> tasks = dao.getByJobid(jobid);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle","Task Chart");
      List list = new ArrayList();
      for(Task task : tasks) {
        Map<String, Object> content = new HashMap<>();
        JsonElement jelement = new JsonParser().parse(task.getDetailInfo());
        Gson gson = new Gson();
        LinkedHashMap<String, Double> json = gson.fromJson(jelement, new TypeToken<LinkedHashMap>(){}
                                                 .getType());
        content.put("taskid", task.getTaskid());
        content.put("tasktype",task.getTaskType());
        content.put("startTime",BigDecimal.valueOf(json.get("startTime")).longValue());
        content.put("finishTime",BigDecimal.valueOf(json.get("finishTime")).longValue());
        list.add(content);
      }
      String data = JSONValue.toJSONString(list);
      attributes.put("data", data);
      return new ModelAndView(attributes, "job_chart.ftl");
    }, new FreeMarkerEngine());

    get("/mapoutput/:jobid", (request, response) -> {
      String jobid = request.params(":jobid");
      TaskDAO dao = new TaskDAO();
      List<Task> tasks = dao.getByJobid(jobid);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Map Task Output");
      List<LinkedHashMap> outputMap = new ArrayList<>();
      List tasksJson = Graphviz.JsonTasks(tasks,outputMap);
      attributes.put("taskid", tasks.get(0).getTaskid().substring(0,24));
      attributes.put("data", JSONValue.toJSONString(tasksJson));
      attributes.put("links", JSONValue.toJSONString(outputMap));
      return new ModelAndView(attributes, "task_io_chart.ftl");
    }, new FreeMarkerEngine());


    get("/login/:info", (request, response) -> {
      String info = request.params(":info");
      if(info.equals("sign")) {
        String username = request.queryParams("username");
        String password = request.queryParams("password");
        AdminDao dao = new AdminDao();
        boolean isSucc = dao.verification(username, password);
        if (isSucc) {
          response.redirect("/admin");
        } else {
          response.redirect("/login/failed");
        }
      }
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("info", info);
      attributes.put("subTitle", "Administration Login");
      return new ModelAndView(attributes, "login.ftl");
    }, new FreeMarkerEngine());

    get("/admin", (request, response) -> {
      Map<String, Object> attributes = new HashMap<>();
      request.session(true);
      request.session().attribute("user","admin");
      attributes.put("subTitle", "Database Manager");
      return new ModelAndView(attributes, "admin.ftl");
    }, new FreeMarkerEngine());
  }

}