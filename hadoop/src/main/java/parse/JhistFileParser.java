package parse;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.jobhistory.*;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangyun on 4/27/15.
 */
public class JhistFileParser implements HistoryEventHandler {
  private static final Log LOG = LogFactory.getLog(JobHistoryParser.class);

  private final FSDataInputStream in;
  private JobInfoQ info = null;

  private IOException parseException = null;

  /**
   * Create the job history parser for the given history file using the
   * given file system
   *
   * @param fs
   * @param historyFile
   * @throws java.io.IOException
   */
  public JhistFileParser(FileSystem fs, Path historyFile) throws IOException {
    this.in = fs.open(historyFile);
  }


  public synchronized JobInfoQ parse() throws IOException {
    EventReader  reader = new EventReader(in);
    if (info != null) {
      return info;
    }
    info = new JobInfoQ();
    int eventCtr = 0;
    HistoryEvent event;
    try {
      while ((event = reader.getNextEvent()) != null) {
        this.handleEvent(event);
        ++eventCtr;
      }
    } catch (IOException ioe) {
      LOG.info("Caught exception parsing history file after " + eventCtr +
              " events", ioe);
      parseException = ioe;
    } finally {
      in.close();
    }
    return info;
  }
  public interface LogInfo{
    public void printAll();
  }

  public class JobInfoQ implements LogInfo{
    /**  workflow information */
    String workflowId;
    String workflowName;
    String workflowNodeName;
    String workflowAdjacencies;
    String workflowTags;

    String errorInfo = "";
    long submitTime;
    long finishTime;
    JobID jobid;
    String username;
    String jobname;
    String jobQueueName;
    String jobConfPath;
    long launchTime;
    int totalMaps;
    int totalReduces;
    int failedMaps;
    int failedReduces;
    int finishedMaps;
    int finishedReduces;
    String jobStatus;
    Counters totalCounters;
    Counters mapCounters;
    Counters reduceCounters;
    JobPriority priority;
    Map<JobACL, AccessControlList> jobACLs;
    boolean uberized;
    //TaskID -> TaskInfo
    Map<String, TaskInfo> tasksMap;
    Map<TaskAttemptID, TaskAttemptInfo> completedTaskAttemptsMap;

    /**  Intended for used store Job info of each query*/
    public JobInfoQ(){
      workflowId = workflowName = workflowNodeName =
              workflowAdjacencies = workflowTags = "";
      submitTime = launchTime = finishTime = -1;
      totalMaps = totalReduces = failedMaps = failedReduces = 0;
      finishedMaps = finishedReduces = 0;
      username = jobname = jobConfPath = jobQueueName = "";
      jobACLs = new HashMap<JobACL, AccessControlList>();
      priority = JobPriority.NORMAL;
      tasksMap = new HashMap<String, TaskInfo>();
      completedTaskAttemptsMap = new HashMap<TaskAttemptID, TaskAttemptInfo>();
    }

    /** Print all the job information for test */
    public void printAll() {
      System.out.println("JOBNAME: " + jobname);
      System.out.println("WORKFLOWID: " + workflowId);
      System.out.println("WORKFLOWNAME: " + workflowName);
      System.out.println("WORKFLOWNODENAME: " + workflowNodeName);
      System.out.println("WORKFLOWADJACENCIES: " + workflowAdjacencies);
      System.out.println("WORKFLOWTAGS: " + workflowTags);
      System.out.println("USERNAME: " + username);
      System.out.println("JOB_QUEUE_NAME: " + jobQueueName);
      System.out.println("SUBMIT_TIME" + submitTime);
      System.out.println("LAUNCH_TIME: " + launchTime);
      System.out.println("JOB_STATUS: " + jobStatus);
      System.out.println("PRIORITY: " + priority);
      System.out.println("TOTAL_MAPS: " + totalMaps);
      System.out.println("TOTAL_REDUCES: " + totalReduces);
      if (mapCounters != null) {

        System.out.println("MAP_COUNTERS:" + mapCounters.toString());
      }
      if (reduceCounters != null) {
        System.out.println("REDUCE_COUNTERS:" + reduceCounters.toString());
      }
      if (totalCounters != null) {
        System.out.println("TOTAL_COUNTERS: " + totalCounters.toString());
      }
      System.out.println("UBERIZED: " + uberized);
    }


    public String getWorkflowId() {
      return workflowId;
    }

    public String getWorkflowName() {
      return workflowName;
    }

    public String getWorkflowNodeName() {
      return workflowNodeName;
    }

    public String getWorkflowAdjacencies() {
      return workflowAdjacencies;
    }

    public String getWorkflowTags() {
      return workflowTags;
    }

    public String getErrorInfo() {
      return errorInfo;
    }

    public long getSubmitTime() {
      return submitTime;
    }

    public long getFinishTime() {
      return finishTime;
    }

    public JobID getJobid() {
      return jobid;
    }

    public String getUsername() {
      return username;
    }

    public String getJobname() {
      return jobname;
    }

    public String getJobQueueName() {
      return jobQueueName;
    }

    public String getJobConfPath() {
      return jobConfPath;
    }

    public long getLaunchTime() {
      return launchTime;
    }

    public int getTotalMaps() {
      return totalMaps;
    }

    public int getTotalReduces() {
      return totalReduces;
    }

    public int getFailedMaps() {
      return failedMaps;
    }

    public int getFailedReduces() {
      return failedReduces;
    }

    public int getFinishedMaps() {
      return finishedMaps;
    }

    public int getFinishedReduces() {
      return finishedReduces;
    }

    public String getJobStatus() {
      return jobStatus;
    }

    public Counters getTotalCounters() {
      return totalCounters;
    }

    public Counters getMapCounters() {
      return mapCounters;
    }

    public Counters getReduceCounters() {
      return reduceCounters;
    }

    public JobPriority getPriority() {
      return priority;
    }

    public Map<JobACL, AccessControlList> getJobACLs() {
      return jobACLs;
    }

    public Map<String, TaskInfo> getTasksMap() {
      return tasksMap;
    }

    public Map<TaskAttemptID, TaskAttemptInfo> getCompletedTaskAttemptsMap() {
      return completedTaskAttemptsMap;
    }
  }

  private void handleJobSubmittedEvent( JobSubmittedEvent event){
    Object datum = event.getDatum();
    JobSubmitted datumJS = ((JobSubmitted)datum);
    info.workflowId = datumJS.getWorkflowId().toString();
    info.workflowAdjacencies = datumJS.getWorkflowAdjacencies().toString();
    info.workflowName = datumJS.getWorkflowName().toString();
    info.workflowNodeName = datumJS.getWorkflowNodeName().toString();
    info.workflowTags = datumJS.getWorkflowTags().toString();

    info.jobid = event.getJobId();
    info.jobname = event.getJobName();
    info.username = StringInterner.weakIntern(event.getUserName());
    info.submitTime = event.getSubmitTime();
    info.jobConfPath = event.getJobConfPath();
    info.jobACLs = event.getJobAcls();
    info.jobQueueName = StringInterner.weakIntern(event.getJobQueueName());
  }

  private void handleJobInfoChangeEvent(JobInfoChangeEvent event) {
    info.submitTime = event.getSubmitTime();
    info.launchTime = event.getLaunchTime();
  }

  private void handleJobFinishedEvent(JobFinishedEvent event) {
    info.finishTime = event.getFinishTime();
    info.finishedMaps = event.getFinishedMaps();
    info.finishedReduces = event.getFinishedReduces();
    info.failedMaps = event.getFailedMaps();
    info.failedReduces = event.getFailedReduces();
    info.totalCounters = event.getTotalCounters();
    info.mapCounters = event.getMapCounters();
    info.reduceCounters = event.getReduceCounters();
    info.jobStatus = org.apache.hadoop.mapred.JobStatus.getJobRunState(org.apache.hadoop.mapred.JobStatus.SUCCEEDED);
  }

  private void handleJobFailedEvent(JobUnsuccessfulCompletionEvent event) {
    info.finishTime = event.getFinishTime();
    info.finishedMaps = event.getFinishedMaps();
    info.finishedReduces = event.getFinishedReduces();
    info.jobStatus = StringInterner.weakIntern(event.getStatus());
    info.errorInfo = StringInterner.weakIntern(event.getDiagnostics());
  }

  private void handleJobPriorityChangeEvent(JobPriorityChangeEvent event) {
    info.priority = event.getPriority();
  }

  private void handleJobQueueChangeEvent(JobQueueChangeEvent event) {
    info.jobQueueName = event.getJobQueueName();
  }

  private void handleJobInitedEvent(JobInitedEvent event) {
    info.launchTime = event.getLaunchTime();
    info.totalMaps = event.getTotalMaps();
    info.totalReduces = event.getTotalReduces();
    info.uberized = event.getUberized();
  }

  public synchronized void handleEvent(HistoryEvent event)  {
    EventType type = event.getEventType();
    switch (type) {
      case JOB_SUBMITTED:
        handleJobSubmittedEvent((JobSubmittedEvent)event);
        break;
      case JOB_STATUS_CHANGED:
        break;
      case JOB_INFO_CHANGED:
        handleJobInfoChangeEvent((JobInfoChangeEvent) event);
        break;
      case JOB_INITED:
        handleJobInitedEvent((JobInitedEvent) event);
        break;
      case JOB_PRIORITY_CHANGED:
        handleJobPriorityChangeEvent((JobPriorityChangeEvent) event);
        break;
      case JOB_QUEUE_CHANGED:
        handleJobQueueChangeEvent((JobQueueChangeEvent) event);
        break;
      case JOB_FAILED:
      case JOB_KILLED:
      case JOB_ERROR:
        handleJobFailedEvent((JobUnsuccessfulCompletionEvent) event);
        break;
      case JOB_FINISHED:
        handleJobFinishedEvent((JobFinishedEvent)event);
        break;
      case TASK_STARTED:
        handleTaskStartedEvent((TaskStartedEvent) event);
        break;
      case TASK_FAILED:
        handleTaskFailedEvent((TaskFailedEvent) event);
        break;
      case TASK_UPDATED:
        handleTaskUpdatedEvent((TaskUpdatedEvent) event);
        break;
      case TASK_FINISHED:
        handleTaskFinishedEvent((TaskFinishedEvent) event);
        break;
      case MAP_ATTEMPT_STARTED:
      case CLEANUP_ATTEMPT_STARTED:
      case REDUCE_ATTEMPT_STARTED:
      case SETUP_ATTEMPT_STARTED:
        handleTaskAttemptStartedEvent((TaskAttemptStartedEvent) event);
        break;
      case MAP_ATTEMPT_FAILED:
      case CLEANUP_ATTEMPT_FAILED:
      case REDUCE_ATTEMPT_FAILED:
      case SETUP_ATTEMPT_FAILED:
      case MAP_ATTEMPT_KILLED:
      case CLEANUP_ATTEMPT_KILLED:
      case REDUCE_ATTEMPT_KILLED:
      case SETUP_ATTEMPT_KILLED:
        handleTaskAttemptFailedEvent(
                (TaskAttemptUnsuccessfulCompletionEvent) event);
        break;
      case MAP_ATTEMPT_FINISHED:
        handleMapAttemptFinishedEvent((MapAttemptFinishedEvent) event);
        break;
      case REDUCE_ATTEMPT_FINISHED:
        handleReduceAttemptFinishedEvent((ReduceAttemptFinishedEvent) event);
        break;
      default:
        //Extended part
    }
  }

  /**
   * TaskInformation is aggregated in this class after parsing
   */
  public class TaskInfo implements LogInfo{
    TaskID taskId;
    long startTime;
    long finishTime;
    TaskType taskType;
    String splitLocations;
    Counters counters;
    String status;
    String error;
    TaskAttemptID failedDueToAttemptId;
    TaskAttemptID successfulAttemptId;
    Map<TaskAttemptID, TaskAttemptInfo> attemptsMap;

    public TaskInfo() {
      startTime = finishTime = -1;
      error = splitLocations = "";
      attemptsMap = new HashMap<TaskAttemptID, TaskAttemptInfo>();
    }

    public void printAll() {
      System.out.println("TASK_ID:" + taskId.toString());
      System.out.println("START_TIME: " + startTime);
      System.out.println("FINISH_TIME:" + finishTime);
      System.out.println("TASK_TYPE:" + taskType);
      System.out.println("SPLIT_LOCATION: " + splitLocations);
      if (counters != null) {
        System.out.println("COUNTERS:" + counters.toString());
      }
      for (TaskAttemptID id: attemptsMap.keySet()) {
        attemptsMap.get(id).printAll();
      }
    }

    /** @return the Task ID */
    public TaskID getTaskId() { return taskId; }
    /** @return the start time of this task */
    public long getStartTime() { return startTime; }
    /** @return the finish time of this task */
    public long getFinishTime() { return finishTime; }
    /** @return the task type */
    public TaskType getTaskType() { return taskType; }
    /** @return the split locations */
    public String getSplitLocations() { return splitLocations; }
    /** @return the counters for this task */
    public Counters getCounters() { return counters; }
    /** @return the task status */
    public String getTaskStatus() { return status; }
    /** @return the attempt Id that caused this task to fail */
    public TaskAttemptID getFailedDueToAttemptId() {
      return failedDueToAttemptId;
    }
    /** @return the attempt Id that caused this task to succeed */
    public TaskAttemptID getSuccessfulAttemptId() {
      return successfulAttemptId;
    }
    /** @return the error */
    public String getError() { return error; }
    /** @return the map of all attempts for this task */
    public Map<TaskAttemptID, TaskAttemptInfo> getAllTaskAttempts() {
      return attemptsMap;
    }
  }

  /**
   * Task Attempt Information is aggregated in this class after parsing
   */
  public class TaskAttemptInfo {
    TaskAttemptID attemptId;
    long startTime;
    long finishTime;
    long shuffleFinishTime;
    long sortFinishTime;
    long mapFinishTime;
    String error;
    String status;
    String state;
    TaskType taskType;
    String trackerName;
    Counters counters;
    int httpPort;
    int shufflePort;
    String hostname;
    int port;
    String rackname;
    ContainerId containerId;

    /** Create a Task Attempt Info which will store attempt level information
     * on a history parse.
     */
    public TaskAttemptInfo() {
      startTime = finishTime = shuffleFinishTime = sortFinishTime =
              mapFinishTime = -1;
      error =  state =  trackerName = hostname = rackname = "";
      port = -1;
      httpPort = -1;
      shufflePort = -1;
    }
    /**
     * Print all the information about this attempt.
     */
    public void printAll() {
      System.out.println("ATTEMPT_ID:" + attemptId.toString());
      System.out.println("START_TIME: " + startTime);
      System.out.println("FINISH_TIME:" + finishTime);
      System.out.println("ERROR:" + error);
      System.out.println("TASK_STATUS:" + status);
      System.out.println("STATE:" + state);
      System.out.println("TASK_TYPE:" + taskType);
      System.out.println("TRACKER_NAME:" + trackerName);
      System.out.println("HTTP_PORT:" + httpPort);
      System.out.println("SHUFFLE_PORT:" + shufflePort);
      System.out.println("CONTIANER_ID:" + containerId);
      if (counters != null) {
        System.out.println("COUNTERS:" + counters.toString());
      }
    }

    /** @return the attempt Id */
    public TaskAttemptID getAttemptId() { return attemptId; }
    /** @return the start time of the attempt */
    public long getStartTime() { return startTime; }
    /** @return the finish time of the attempt */
    public long getFinishTime() { return finishTime; }
    /** @return the shuffle finish time. Applicable only for reduce attempts */
    public long getShuffleFinishTime() { return shuffleFinishTime; }
    /** @return the sort finish time. Applicable only for reduce attempts */
    public long getSortFinishTime() { return sortFinishTime; }
    /** @return the map finish time. Applicable only for map attempts */
    public long getMapFinishTime() { return mapFinishTime; }
    /** @return the error string */
    public String getError() { return error; }
    /** @return the state */
    public String getState() { return state; }
    /** @return the task status */
    public String getTaskStatus() { return status; }
    /** @return the task type */
    public TaskType getTaskType() { return taskType; }
    /** @return the tracker name where the attempt executed */
    public String getTrackerName() { return trackerName; }
    /** @return the host name */
    public String getHostname() { return hostname; }
    /** @return the port */
    public int getPort() { return port; }
    /** @return the rack name */
    public String getRackname() { return rackname; }
    /** @return the counters for the attempt */
    public Counters getCounters() { return counters; }
    /** @return the HTTP port for the tracker */
    public int getHttpPort() { return httpPort; }
    /** @return the Shuffle port for the tracker */
    public int getShufflePort() { return shufflePort; }
    /** @return the ContainerId for the tracker */
    public ContainerId getContainerId() { return containerId; }
  }

  private void handleTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId().toString());
    TaskAttemptInfo attemptInfo =
            taskInfo.attemptsMap.get(event.getAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
 //   attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleReduceAttemptFinishedEvent
          (ReduceAttemptFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId().toString());
    TaskAttemptInfo attemptInfo =
            taskInfo.attemptsMap.get(event.getAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
    attemptInfo.shuffleFinishTime = event.getShuffleFinishTime();
    attemptInfo.sortFinishTime = event.getSortFinishTime();
//    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleMapAttemptFinishedEvent(MapAttemptFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId().toString());
    TaskAttemptInfo attemptInfo =
            taskInfo.attemptsMap.get(event.getAttemptId());
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
    attemptInfo.mapFinishTime = event.getMapFinishTime();
//    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleTaskAttemptFailedEvent(
          TaskAttemptUnsuccessfulCompletionEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId().toString());
    if(taskInfo == null) {
      LOG.warn("TaskInfo is null for TaskAttemptUnsuccessfulCompletionEvent"
              + " taskId:  " + event.getTaskId().toString());
      return;
    }
    TaskAttemptInfo attemptInfo =
            taskInfo.attemptsMap.get(event.getTaskAttemptId());
    if(attemptInfo == null) {
      LOG.warn("AttemptInfo is null for TaskAttemptUnsuccessfulCompletionEvent"
              + " taskAttemptId:  " + event.getTaskAttemptId().toString());
      return;
    }
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.error = StringInterner.weakIntern(event.getError());
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    attemptInfo.shuffleFinishTime = event.getFinishTime();
    attemptInfo.sortFinishTime = event.getFinishTime();
    attemptInfo.mapFinishTime = event.getFinishTime();
  //  attemptInfo.counters = event.getCounters();
    if(TaskStatus.State.SUCCEEDED.toString().equals(taskInfo.status))
    {
      //this is a successful task
      if(attemptInfo.getAttemptId().equals(taskInfo.getSuccessfulAttemptId()))
      {
        // the failed attempt is the one that made this task successful
        // so its no longer successful. Reset fields set in
        // handleTaskFinishedEvent()
        taskInfo.counters = null;
        taskInfo.finishTime = -1;
        taskInfo.status = null;
        taskInfo.successfulAttemptId = null;
      }
    }
    info.completedTaskAttemptsMap.put(event.getTaskAttemptId(), attemptInfo);
  }

  private void handleTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    TaskAttemptID attemptId = event.getTaskAttemptId();
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId().toString());

    TaskAttemptInfo attemptInfo = new TaskAttemptInfo();
    attemptInfo.startTime = event.getStartTime();
    attemptInfo.attemptId = event.getTaskAttemptId();
    attemptInfo.httpPort = event.getHttpPort();
    attemptInfo.trackerName = StringInterner.weakIntern(event.getTrackerName());
    attemptInfo.taskType = event.getTaskType();
    attemptInfo.shufflePort = event.getShufflePort();
    attemptInfo.containerId = event.getContainerId();

    taskInfo.attemptsMap.put(attemptId, attemptInfo);
  }

  private void handleTaskFinishedEvent(TaskFinishedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId().toString());
    taskInfo.counters = event.getCounters();
    taskInfo.finishTime = event.getFinishTime();
    taskInfo.status = TaskStatus.State.SUCCEEDED.toString();
    taskInfo.successfulAttemptId = event.getSuccessfulTaskAttemptId();
  }

  private void handleTaskUpdatedEvent(TaskUpdatedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId().toString());
    taskInfo.finishTime = event.getFinishTime();
  }

  private void handleTaskFailedEvent(TaskFailedEvent event) {
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId().toString());
    taskInfo.status = TaskStatus.State.FAILED.toString();
    taskInfo.finishTime = event.getFinishTime();
    taskInfo.error = StringInterner.weakIntern(event.getError());
    taskInfo.failedDueToAttemptId = event.getFailedAttemptID();
    taskInfo.counters = event.getCounters();
  }

  private void handleTaskStartedEvent(TaskStartedEvent event) {
    TaskInfo taskInfo = new TaskInfo();
    taskInfo.taskId = event.getTaskId();
    taskInfo.startTime = event.getStartTime();
    taskInfo.taskType = event.getTaskType();
    taskInfo.splitLocations = event.getSplitLocations();
    info.tasksMap.put(event.getTaskId().toString(), taskInfo);
  }

}
