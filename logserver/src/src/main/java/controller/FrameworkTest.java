package controller;

/**
 * Created by zhangyun on 5/15/15.
 */

import java.util.HashMap;
import java.util.Map;

import spark.ModelAndView;
import view.FreeMarkerEngine;


import static spark.Spark.get;

public class FrameworkTest {

  public static void main(String args[]) {

    get("/hello", (request, response) -> {
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("message", "Hello World!");

      // The hello.ftl file is located in directory:
      // src/test/resources/spark/template/freemarker
      return new ModelAndView(attributes, "../view/template/hello,ftl");
    }, new FreeMarkerEngine());

  }

}