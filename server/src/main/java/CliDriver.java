/**
 * Created by zhangyun on 5/15/15.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bean.Query;
import dao.QueryDAO;
import spark.ModelAndView;
import spark.Spark;


import static spark.Spark.get;

public class CliDriver {

  public static void main(String args[]) {
    Spark.staticFileLocation("/public");
    get("/", (request, response) -> {
      QueryDAO dao = new QueryDAO();
      List<Query> list = dao.getAll();
      List<Map<String, Object>> allInfo = new ArrayList<Map<String, Object>>();
      for (Query l : list ){
        int i = 1;
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("number", i);
        query.put("username", l.getUsername());
        query.put("queryString" , l.getQueryStirng());
        query.put("jobDependency", l.getJobDependency());
        query.put("jobAmount", l.getJobAmount());
        allInfo.add(query);
      }
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("subTitle", "Query Information");
      attributes.put("allInfo", allInfo);
      // The hello,ftl file is located in directory:
      // src/test/resources/spark/template/freemarker
      return new ModelAndView(attributes, "index.ftl");
    }, new FreeMarkerEngine());
  }

}