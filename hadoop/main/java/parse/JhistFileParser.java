package parse;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.jobhistory.*;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringInterner;

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

  public class JobInfoQ {
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
      default:
        //Extended part
    }
  }
}
