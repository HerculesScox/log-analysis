package util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
}
