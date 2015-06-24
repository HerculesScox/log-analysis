package util;

import bean.Task;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import dao.TaskDAO;
import sun.awt.image.ImageWatched;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by zhangyun on 5/29/15.
 * The Utility class for graphviz
 */
public class Graphviz {

  /**
   * Convert relation of job dependency to String.
   * @param stages
   * @return
   */
  public static String conv( Map<String,String> stages,
      Set<String> markedStages){

    HashSet<String> fixedStages = new HashSet<>();
    for (String s: markedStages){
     fixedStages.add("\""+ s +"\"");
    }

    StringBuilder builder = new StringBuilder();
    builder.append("digraph G {\n");
    for(String k : stages.keySet()){
      for(String inerKey : stages.get(k).replace(",","\",\"").split(",")){
        builder.append( k );
        builder.append( " -> ");
        builder.append( inerKey );
        builder.append( ";\n" );
        if( fixedStages.contains(inerKey)) {
          builder.append(inerKey);
          builder.append("[style=filled,color=red]");
          builder.append( ";\n" );
          fixedStages.remove(inerKey);
        }
      }
      if( fixedStages.contains(k)) {
        builder.append(k);
        builder.append("[style=filled,color=red]");
        builder.append( ";\n" );
        fixedStages.remove(k);
      }
    }
    builder.append("}\n");
    return builder.toString();
  }

  public static  List JsonTasks(List<Task> tasks, List<LinkedHashMap> outputMap){
    List<LinkedHashMap> list = new ArrayList<>();
    Collections.sort(tasks,
            (Task o1, Task o2) -> o1.getTaskid().compareTo(o2.getTaskid())
    );
    int my = 10, ry = 10;
    for(Task task : tasks) {
      LinkedHashMap<String, Object> content = new LinkedHashMap<>();
      content.put("taskid", task.getTaskid());
      content.put("tasktype",task.getTaskType());
      if(task.getTaskType().equals("MAP")){
        content.put("x", 70);
        content.put("y", my);
        my = my + 15;
      }else{
        content.put("x", 660);
        content.put("y", ry);
        ry = ry + 15;
      }
      list.add(content);
    }

    for(Task mtk : tasks) {
      //add map output to which reduce task
      if (mtk.getTaskType().equals("MAP")) {
        LinkedHashMap mc = getElement(list, mtk.getTaskid());
        Gson gson = new Gson();
        for (Task rtk : tasks) {
          if (rtk.getTaskType().equals("REDUCE")) {
            JsonElement jelementTk = new JsonParser().parse(rtk.getDetailInfo());
            LinkedHashMap<String, Object> jsonTk =
                     gson.fromJson(jelementTk, new TypeToken<LinkedHashMap>() {
                    }
                    .getType());
            String matchStr = jsonTk.get("inputMapTasks")
                    .toString()
                    .replace("[", "")
                    .replace("]", "")
                    .replace(" ", "");
            List<String> inputOfRedTask = Arrays.asList(matchStr.split(","));
            if (inputOfRedTask.contains(mtk.getTaskid())) {
              LinkedHashMap<String, LinkedHashMap> oneMap = new LinkedHashMap<>();
              LinkedHashMap rc = getElement(list, rtk.getTaskid());
              oneMap.put("source", mc);
              oneMap.put("target", rc);
              outputMap.add(oneMap);
            }
          }
        }
      }
    }
    return list;
  }

  private static LinkedHashMap getElement(List<LinkedHashMap> list , String taskid){
    for(LinkedHashMap e : list){
      if(e.get("taskid").equals(taskid)){
        return e;
      }
    }
    //Not found!
    return null;
  }


}
