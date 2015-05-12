package base;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.json.simple.JSONObject;
import parse.JhistFileParser;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Job {
  private JhistFileParser.JobInfoQ  jobInfo;
  private Path JHPath;
  private Path confFile;
  private HashMap<TaskID,Task> tasks;
  //All top op of all map tasks
  private HashSet<Node> topOps;


  public Job(JhistFileParser.JobInfoQ jobInfo , Path JHPath , Path confFile) {
    this.jobInfo = jobInfo;
    this.JHPath = JHPath;
    this.confFile = confFile;
    this.topOps = new HashSet<Node>();
    this.tasks = new HashMap<TaskID,Task>();
  }

  public Path getConfFile() {
    return confFile;
  }

  public Path getJHPath() {
    return JHPath;
  }

  public JhistFileParser.JobInfoQ getJobInfo() {
    return jobInfo;
  }

  public HashMap<TaskID, Task> getTasks() {
    return tasks;
  }

  public HashSet<Node> getTopOps() {
    return topOps;
  }

  public void chopKilledTask(){
    Set<TaskID> succTask = tasks.keySet();
    HashSet<TaskAttemptID> redInputTask = new HashSet<TaskAttemptID>();
    for(TaskID id : tasks.keySet()){
      if (tasks.get(id) instanceof ReduceTask){
        ReduceTask rt = (ReduceTask) tasks.get(id);
        for(TaskAttemptID ta : rt.getAttemptTaskID()){
          if(succTask.contains(ta.getTaskID())){
            redInputTask.add(ta);
          }
        }
        rt.setAttemptTaskIDs(redInputTask);
      }
    }
  }


  public String toJSON() throws IOException{
    JSONObject jobJson = new JSONObject();
    jobJson.put("jobID", jobInfo.getJobid());
    jobJson.put("workflowID", jobInfo.getWorkflowId());
    jobJson.put("workflowNodeName", jobInfo.getWorkflowNodeName());
    jobJson.put("submitTime", jobInfo.getSubmitTime());
    jobJson.put("launchTime", jobInfo.getLaunchTime());
    jobJson.put("totalMaps", jobInfo.getTotalMaps());

    LinkedHashMap counterGroup = new LinkedHashMap();
   // LinkedList list = new LinkedList();
    LinkedHashMap mapcounter = new LinkedHashMap();
    LinkedHashMap redcounter = new LinkedHashMap();
    LinkedHashMap totalcounter = new LinkedHashMap();
    boolean hasFilterOperator = false;
    Iterator<String> mapGroups =
            jobInfo.getMapCounters().getGroupNames().iterator();
    while (mapGroups.hasNext()) {
      if(mapGroups.next().matches(".*FilterOperator\\$Counter.*")){
        hasFilterOperator = true;
      }
    }

    mapcounter.put("inputRecords",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.MAP_INPUT_RECORDS.toString()).getValue());
    mapcounter.put("outputRecords",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.MAP_OUTPUT_RECORDS.toString()).getValue());
    mapcounter.put("outputBytes",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.MAP_OUTPUT_BYTES.toString()).getValue());
    mapcounter.put("outputMterializedBytes",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.MAP_OUTPUT_MATERIALIZED_BYTES.toString()).getValue());
    mapcounter.put("mergedMapOutputs",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.MERGED_MAP_OUTPUTS.toString()).getValue());
    mapcounter.put("combineInputRecords",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.COMBINE_INPUT_RECORDS.toString()).getValue());
    mapcounter.put("spilledRecords",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.SPILLED_RECORDS.toString()).getValue());
    mapcounter.put("GCtime",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.GC_TIME_MILLIS.toString()).getValue());
    mapcounter.put("CPUtime",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.CPU_MILLISECONDS.toString()).getValue());
    mapcounter.put("physicalMemory",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.PHYSICAL_MEMORY_BYTES.toString()).getValue());
    mapcounter.put("virtualMemory",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.VIRTUAL_MEMORY_BYTES.toString()).getValue());
    mapcounter.put("heapUsage",
                jobInfo.getMapCounters()
                .getGroup(CounterGroup.TASK_COUNTER.get())
                .findCounter(CounterGroup.COMMITTED_HEAP_BYTES.toString()).getValue());
    if (hasFilterOperator) {
      mapcounter.put("FILfiltered",
              jobInfo.getMapCounters()
                      .getGroup(CounterGroup.FILTE_OPERATOR.get())
                      .findCounter(CounterGroup.FILTERED.toString()).getValue());
      mapcounter.put("FILpassed",
              jobInfo.getMapCounters()
                      .getGroup(CounterGroup.FILTE_OPERATOR.get())
                      .findCounter(CounterGroup.PASSED.toString()).getValue());
    }

    counterGroup.put("mapCounter",mapcounter);

    Iterator<String> redGroups =
            jobInfo.getReduceCounters().getGroupNames().iterator();
    while (redGroups.hasNext()) {
      if(redGroups.next().matches(".*FilterOperator\\$Counter.*")){
        hasFilterOperator = true;
      }else {
        hasFilterOperator = false;
      }
    }
    redcounter.put("combineInputRecords",
                  jobInfo.getReduceCounters()
                  .getGroup(CounterGroup.TASK_COUNTER.get())
                  .findCounter(CounterGroup.COMBINE_INPUT_RECORDS.toString()).getValue());
    redcounter.put("combineOutputRecords",
                   jobInfo.getReduceCounters()
                  .getGroup(CounterGroup.TASK_COUNTER.get())
                  .findCounter(CounterGroup.COMBINE_OUTPUT_RECORDS.toString()).getValue());
    redcounter.put("reduceInputGroups",
                  jobInfo.getReduceCounters()
                  .getGroup(CounterGroup.TASK_COUNTER.get())
                  .findCounter(CounterGroup.REDUCE_INPUT_GROUPS.toString()).getValue());
    redcounter.put("inputRecords",
                   jobInfo.getReduceCounters()
                  .getGroup(CounterGroup.TASK_COUNTER.get())
                  .findCounter(CounterGroup.REDUCE_INPUT_RECORDS.toString()).getValue());
    redcounter.put("outputRecords",
                   jobInfo.getReduceCounters()
                  .getGroup(CounterGroup.TASK_COUNTER.get())
                  .findCounter(CounterGroup.REDUCE_OUTPUT_RECORDS.toString()).getValue());
    redcounter.put("shuffleBytes",
                   jobInfo.getReduceCounters()
                  .getGroup(CounterGroup.TASK_COUNTER.get())
                  .findCounter(CounterGroup.REDUCE_SHUFFLE_BYTES.toString()).getValue());
    redcounter.put("spilledRecords",
                  jobInfo.getReduceCounters()
                  .getGroup(CounterGroup.TASK_COUNTER.get())
                  .findCounter(CounterGroup.SPILLED_RECORDS.toString()).getValue());
    redcounter.put("mergedMapOutputs",
                   jobInfo.getReduceCounters()
                  .getGroup(CounterGroup.TASK_COUNTER.get())
                  .findCounter(CounterGroup.MERGED_MAP_OUTPUTS.toString()).getValue());
    redcounter.put("shuffledMaps",
            jobInfo.getReduceCounters()
                    .getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.SHUFFLED_MAPS.toString()).getValue());
    redcounter.put("GCtime",
            jobInfo.getReduceCounters()
                    .getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.GC_TIME_MILLIS.toString()).getValue());
    redcounter.put("CPUtime",
            jobInfo.getReduceCounters()
                    .getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.CPU_MILLISECONDS.toString()).getValue());
    redcounter.put("physicalMemory",
            jobInfo.getReduceCounters()
                    .getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.PHYSICAL_MEMORY_BYTES.toString()).getValue());
    redcounter.put("virtualMemory",
            jobInfo.getReduceCounters()
                    .getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.VIRTUAL_MEMORY_BYTES.toString()).getValue());
    redcounter.put("heapUsage",
            jobInfo.getReduceCounters()
                    .getGroup(CounterGroup.TASK_COUNTER.get())
                    .findCounter(CounterGroup.COMMITTED_HEAP_BYTES.toString()).getValue());
    if (hasFilterOperator) {
      redcounter.put("FILfiltered",
              jobInfo.getReduceCounters()
                      .getGroup(CounterGroup.FILTE_OPERATOR.get())
                      .findCounter(CounterGroup.FILTERED.toString()).getValue());
      redcounter.put("FILpassed",
              jobInfo.getReduceCounters()
                      .getGroup(CounterGroup.FILTE_OPERATOR.get())
                      .findCounter(CounterGroup.PASSED.toString()).getValue());
    }

    counterGroup.put("redCounter",redcounter);

    totalcounter.put("launchedMaps",
                     jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.TOTAL_LAUNCHED_MAPS.toString()).getValue());
    totalcounter.put("launchedReduce",
                     jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.TOTAL_LAUNCHED_REDUCES.toString()).getValue());
    totalcounter.put("dataLocalMap ",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.DATA_LOCAL_MAPS.toString()).getValue());
    totalcounter.put("rackLocalMap",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.RACK_LOCAL_MAPS.toString()).getValue());
    totalcounter.put("slotsMillsMaps",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.SLOTS_MILLIS_MAPS.toString()).getValue());
    totalcounter.put("slotsMillsReds",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.SLOTS_MILLIS_REDUCES.toString()).getValue());
    totalcounter.put("millsMaps",
                     jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.MILLIS_MAPS.toString()).getValue());
    totalcounter.put("millsReds",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.MILLIS_REDUCES.toString()).getValue());
    totalcounter.put("vcoresMillsMaps",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.VCORES_MILLIS_MAPS.toString()).getValue());
    totalcounter.put("vcoresMillsReds",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.VCORES_MILLIS_REDUCES.toString()).getValue());
    totalcounter.put("mbMillsMaps",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.MB_MILLIS_MAPS.toString()).getValue());
    totalcounter.put("mbMillsReds",
                    jobInfo.getTotalCounters()
                    .getGroup(CounterGroup.JOB_COUNTER.get())
                    .findCounter(CounterGroup.MB_MILLIS_REDUCES.toString()).getValue());


      Iterator<org.apache.hadoop.mapreduce.Counter> c =
              jobInfo.getTotalCounters()
                      .getGroup(CounterGroup.TASK_COUNTER.get()).iterator();
      while (c.hasNext()) {
        org.apache.hadoop.mapreduce.Counter n = c.next();
        System.out.println(n.getName()+" => " );
      }
      System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");




    counterGroup.put("totalCounter",totalcounter);
    jobJson.put("Counter",counterGroup);

    StringWriter out = new StringWriter();
    jobJson.writeJSONString(out);

    return jobJson.toJSONString();
  }
}
