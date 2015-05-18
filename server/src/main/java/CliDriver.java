/**
 * Created by zhangyun on 5/15/15.
 */

import java.util.HashMap;
import java.util.Map;

import spark.ModelAndView;
import spark.Spark;


import static spark.Spark.get;

public class CliDriver {

  public static void main(String args[]) {
    Spark.staticFileLocation("/public");
    get("/", (request, response) -> {
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("message", "Query Information");
      // The hello,ftl file is located in directory:
      // src/test/resources/spark/template/freemarker
      return new ModelAndView(attributes, "index.ftl");
    }, new FreeMarkerEngine());
  }

}