package util;

import base.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;


import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Created by zhangyun on 5/13/15.
 */
public class FactorStatistics {

  private static boolean hasFilterOperator(Counters parseResult){
    boolean hasFilterOperator = false;
    Iterator<String> mapGroups =
            parseResult.getGroupNames().iterator();
    while (mapGroups.hasNext()) {
      if(mapGroups.next().matches(".*FilterOperator\\$Counter.*")){
        hasFilterOperator = true;
      }
    }
    return hasFilterOperator;

  }

  /**
   * Only statistics for task counter
   * @param mapcounter
   * @param parseResult
   */
  @SuppressWarnings("unchecked")
  public static void mapCounter( LinkedHashMap mapcounter,Counters parseResult){

    boolean hasFilterOperator = hasFilterOperator(parseResult);
    mapcounter.put("inputRecords",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.MAP_INPUT_RECORDS.toString()).getValue());
    mapcounter.put("outputRecords",
                     parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.MAP_OUTPUT_RECORDS.toString()).getValue());
    mapcounter.put("outputBytes",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.MAP_OUTPUT_BYTES.toString()).getValue());
    mapcounter.put("outputMterializedBytes",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.MAP_OUTPUT_MATERIALIZED_BYTES.toString()).getValue());
    mapcounter.put("mergedMapOutputs",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.MERGED_MAP_OUTPUTS.toString()).getValue());
    mapcounter.put("combineInputRecords",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.COMBINE_INPUT_RECORDS.toString()).getValue());
    mapcounter.put("spilledRecords",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.SPILLED_RECORDS.toString()).getValue());
    mapcounter.put("GCtime",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.GC_TIME_MILLIS.toString()).getValue());
    mapcounter.put("CPUtime",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.CPU_MILLISECONDS.toString()).getValue());
    mapcounter.put("physicalMemory",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.PHYSICAL_MEMORY_BYTES.toString()).getValue());
    mapcounter.put("virtualMemory",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.VIRTUAL_MEMORY_BYTES.toString()).getValue());
    mapcounter.put("heapUsage",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.COMMITTED_HEAP_BYTES.toString()).getValue());
    if (hasFilterOperator) {
      mapcounter.put("FILfiltered",
                      parseResult.getGroup(CounterGroup.FILTE_OPERATOR.get())
                      .findCounter(CounterGroup.FILTERED.toString()).getValue());
      mapcounter.put("FILpassed",
                      parseResult.getGroup(CounterGroup.FILTE_OPERATOR.get())
                      .findCounter(CounterGroup.PASSED.toString()).getValue());
    }
  }

  /**
   * Only statistics for task counter
   * @param redcounter
   * @param parseResult
   */
  @SuppressWarnings("unchecked")
  public static void reduceCounter( LinkedHashMap redcounter,Counters parseResult){

    boolean hasFilterOperator = hasFilterOperator(parseResult);

    redcounter.put("combineInputRecords",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.COMBINE_INPUT_RECORDS.toString()).getValue());
    redcounter.put("combineOutputRecords",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.COMBINE_OUTPUT_RECORDS.toString()).getValue());
    redcounter.put("reduceInputGroups",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.REDUCE_INPUT_GROUPS.toString()).getValue());
    redcounter.put("inputRecords",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.REDUCE_INPUT_RECORDS.toString()).getValue());
    redcounter.put("outputRecords",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.REDUCE_OUTPUT_RECORDS.toString()).getValue());
    redcounter.put("shuffleBytes",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.REDUCE_SHUFFLE_BYTES.toString()).getValue());
    redcounter.put("spilledRecords",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.SPILLED_RECORDS.toString()).getValue());
    redcounter.put("mergedMapOutputs",
                     parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.MERGED_MAP_OUTPUTS.toString()).getValue());
    redcounter.put("shuffledMaps",
                     parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.SHUFFLED_MAPS.toString()).getValue());
    redcounter.put("GCtime",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.GC_TIME_MILLIS.toString()).getValue());
    redcounter.put("CPUtime",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.CPU_MILLISECONDS.toString()).getValue());
    redcounter.put("physicalMemory",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.PHYSICAL_MEMORY_BYTES.toString()).getValue());
    redcounter.put("virtualMemory",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.VIRTUAL_MEMORY_BYTES.toString()).getValue());
    redcounter.put("heapUsage",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.COMMITTED_HEAP_BYTES.toString()).getValue());
    if (hasFilterOperator) {
      redcounter.put("FILfiltered",
                      parseResult.getGroup(CounterGroup.FILTE_OPERATOR.get())
                      .findCounter(CounterGroup.FILTERED.toString()).getValue());
      redcounter.put("FILpassed",
                      parseResult.getGroup(CounterGroup.FILTE_OPERATOR.get())
                      .findCounter(CounterGroup.PASSED.toString()).getValue());
    }

  }

  /**
   * Statictics fro job counter and task counter
   * @param totalcounter
   * @param parseResult
   */
  @SuppressWarnings("unchecked")
  public static void totalCounter( LinkedHashMap totalcounter,Counters parseResult){
    /** JOBCOUNTER */
    totalcounter.put("launchedMaps",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.TOTAL_LAUNCHED_MAPS.toString()).getValue());
    totalcounter.put("launchedReduce",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.TOTAL_LAUNCHED_REDUCES.toString()).getValue());
    totalcounter.put("dataLocalMap ",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.DATA_LOCAL_MAPS.toString()).getValue());
    totalcounter.put("otherMap",
                     parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.OTHER_LOCAL_MAPS.toString()).getValue());
    totalcounter.put("rackLocalMap",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.RACK_LOCAL_MAPS.toString()).getValue());
    totalcounter.put("slotsMillsMaps",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.SLOTS_MILLIS_MAPS.toString()).getValue());
    totalcounter.put("slotsMillsReds",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.SLOTS_MILLIS_REDUCES.toString()).getValue());
    totalcounter.put("millsMaps",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.MILLIS_MAPS.toString()).getValue());
    totalcounter.put("millsReds",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.MILLIS_REDUCES.toString()).getValue());
    totalcounter.put("vcoresMillsMaps",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.VCORES_MILLIS_MAPS.toString()).getValue());
    totalcounter.put("vcoresMillsReds",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.VCORES_MILLIS_REDUCES.toString()).getValue());
    totalcounter.put("mbMillsMaps",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.MB_MILLIS_MAPS.toString()).getValue());
    totalcounter.put("mbMillsReds",
                    parseResult.getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.MB_MILLIS_REDUCES.toString()).getValue());
    /** TASK COUNTER*/
    totalcounter.put("GCtime",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.GC_TIME_MILLIS.toString()).getValue());
    totalcounter.put("CPUtime",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.CPU_MILLISECONDS.toString()).getValue());
    totalcounter.put("physicalMemory",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.PHYSICAL_MEMORY_BYTES.toString()).getValue());
    totalcounter.put("virtualMemory",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.VIRTUAL_MEMORY_BYTES.toString()).getValue());
    totalcounter.put("heapUsage",
                    parseResult.getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.COMMITTED_HEAP_BYTES.toString()).getValue());

  }

}
