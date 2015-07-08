package util;

import bean.Job;
import bean.Query;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import dao.JobDAO;
import dao.QueryDAO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyun on 7/6/15.
 */
public class GenRenderData {
  public static  List<Map<String, Object>> mainPage(int page,Map<String, Object> attributes,String username){
    int maxSize = 10;
    QueryDAO dao = new QueryDAO();
    List<Query> list = dao.getAll(page,maxSize,username);
    List<Map<String, Object>> allInfo = new ArrayList<>();
    int count = dao.getQueryNums(username);
    attributes.put("page",page);
    attributes.put("count",count);
    attributes.put("maxSize", maxSize);
    int i = (page - 1) * maxSize +1 ;
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

    return allInfo;
  }


  public static  List<Map<String, Object>> jobsList(String workflowID) {
    JobDAO dao = new JobDAO();
    List<Job> jobs = dao.getByWfID(workflowID);
    List<Map<String, Object>> allInfo = new ArrayList<>();
    int i = 1;
    for (Job job : jobs) {
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
    return  allInfo;
  }
}
