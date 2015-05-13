package base;


/**
 * Created by zhangyun on 5/12/15.
 *
 * Counter groups:
 * org.apache.hadoop.mapreduce.TaskCounter
 * org.apache.hadoop.hive.ql.exec.FilterOperator$Counter
 * org.apache.hadoop.hive.ql.exec.MapOperator$Counter
 * org.apache.hadoop.hive.ql.exec.Operator$ProgressCounter
 * org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter
 * org.apache.hadoop.mapreduce.JobCounter
 * org.apache.hadoop.mapred.JobInProgress$Counter
 */

public enum CounterGroup {
  TASK_COUNTER("org.apache.hadoop.mapreduce.TaskCounter"),
  /** Counters of taskCounter group */
  MAP_INPUT_RECORDS,
  MAP_OUTPUT_RECORDS,
  MAP_OUTPUT_BYTES,
  MAP_OUTPUT_MATERIALIZED_BYTES,
  SPLIT_RAW_BYTES,
  COMBINE_INPUT_RECORDS,
  SPILLED_RECORDS,
  FAILED_SHUFFLE,
  MERGED_MAP_OUTPUTS,
  GC_TIME_MILLIS,
  CPU_MILLISECONDS,
  PHYSICAL_MEMORY_BYTES,
  VIRTUAL_MEMORY_BYTES,
  COMMITTED_HEAP_BYTES,

  /** reduce task counter*/
  COMBINE_OUTPUT_RECORDS,
  REDUCE_INPUT_GROUPS,
  REDUCE_SHUFFLE_BYTES,
  REDUCE_INPUT_RECORDS,
  REDUCE_OUTPUT_RECORDS,
  SHUFFLED_MAPS,

  FILTE_OPERATOR("org.apache.hadoop.hive.ql.exec.FilterOperator$Counter"),
  /** Counters of FilterOperator$Counter group */
  FILTERED,
  PASSED,

  MAP_OPERATOR("org.apache.hadoop.hive.ql.exec.MapOperator$Counter"),

  OPERATOR_PROJRESS("org.apache.hadoop.hive.ql.exec.Operator$ProgressCounter"),

  FILE_INPUT_FORMAT("org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter"),

  JOB_PROGRESS("org.apache.hadoop.mapred.JobInProgress$Counter"),

  JOB_COUNTER("org.apache.hadoop.mapreduce.JobCounter"),
  /** total counters */
  TOTAL_LAUNCHED_MAPS,
  TOTAL_LAUNCHED_REDUCES,
  OTHER_LOCAL_MAPS,
  DATA_LOCAL_MAPS,
  RACK_LOCAL_MAPS,
  SLOTS_MILLIS_MAPS,
  SLOTS_MILLIS_REDUCES,
  MILLIS_MAPS,
  MILLIS_REDUCES,
  VCORES_MILLIS_MAPS,
  VCORES_MILLIS_REDUCES,
  MB_MILLIS_MAPS,
  MB_MILLIS_REDUCES;

  private String content;

  private CounterGroup(){

  }

  private CounterGroup(String content){
    this.content = content;
  }

  public String get(){
    return  content;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
