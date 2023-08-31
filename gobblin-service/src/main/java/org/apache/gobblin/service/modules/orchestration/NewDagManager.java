/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor;
import org.apache.gobblin.service.monitoring.KillFlowEvent;
import org.apache.gobblin.service.monitoring.ResumeFlowEvent;
import org.apache.gobblin.service.monitoring.event.JobStatusEvent;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.service.ExecutionStatus.*;


/**
 * NewDagManager has these functionalities :
 * a) manages {@link Dag}s through {@link DagManagementStateStore}.
 * b) subscribes to {@link JobStatusEvent} sent by {@link KafkaJobStatusMonitor}
 * c) spawns a {@link KillDagThread} that enforces run time and job start time deadlines.
 * d) spawns a {@link DagManager.FailedDagRetentionThread} that cleans failed dags.
 * e) load {@link Dag}s on service-start / set-active.
 */
@Slf4j
public class NewDagManager implements DagManagement {
  public static final String DAG_MANAGER_PREFIX = "gobblin.service.dagManager.";
  public static final Integer DEFAULT_NUM_THREADS = 3;
  public static final String NUM_THREADS_KEY = DAG_MANAGER_PREFIX + "numThreads";
  private static final String DAG_STATESTORE_CLASS_KEY = DAG_MANAGER_PREFIX + "dagStateStoreClass";
  private static final String FAILED_DAG_STATESTORE_PREFIX = "failedDagStateStore";
  private static final String FAILED_DAG_RETENTION_TIME_UNIT = FAILED_DAG_STATESTORE_PREFIX + ".retention.timeUnit";
  private static final String DEFAULT_FAILED_DAG_RETENTION_TIME_UNIT = "DAYS";
  private static final String FAILED_DAG_RETENTION_TIME = FAILED_DAG_STATESTORE_PREFIX + ".retention.time";
  private static final long DEFAULT_FAILED_DAG_RETENTION_TIME = 7L;
  // Re-emit the final flow status if not detected within 5 minutes
  public static final String FAILED_DAG_POLLING_INTERVAL = FAILED_DAG_STATESTORE_PREFIX + ".retention.pollingIntervalMinutes";
  public static final Integer DEFAULT_FAILED_DAG_POLLING_INTERVAL = 60;
  // Default job start SLA time if configured, measured in minutes. Default is 10 minutes
  private static final String JOB_START_SLA_TIME = DAG_MANAGER_PREFIX + ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME;
  private static final String JOB_START_SLA_UNITS = DAG_MANAGER_PREFIX + ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME_UNIT;
  private static final int INITIAL_HOUSEKEEPING_THREAD_DELAY = 2;
  private final Config config;
  private final Integer retentionPollingInterval;

  private final ScheduledExecutorService scheduledExecutorPool;
  @Getter private final DagStateStore failedDagStateStore;
  @Getter private Set<String> failedDagIds;
  private Map<URI, TopologySpec> topologySpecMap = new HashMap<>();
  @Getter private DagStateStore dagStateStore;
  protected final Long defaultJobStartSlaTimeMillis;
  @Getter private final JobStatusRetriever jobStatusRetriever;
  @Getter private final UserQuotaManager quotaManager;
  @Getter private final Optional<Timer> jobStatusPolledTimer;
  @Getter private final Optional<EventSubmitter> eventSubmitter;
  private final long failedDagRetentionTime;
  @Getter private final DagManagerMetrics dagManagerMetrics;
  @Getter private final DagProcessingEngine dagProcessingEngine;

  @Inject(optional=true)
  protected Optional<DagActionStore> dagActionStore;
  @Inject(optional=true)
  DagManagementStateStore dagManagementStateStore;
  private static final int MAX_HOUSEKEEPING_THREAD_DELAY = 180;
  protected final EventBus eventBus;

  public NewDagManager(Config config, JobStatusRetriever jobStatusRetriever, Optional<DagActionStore> dagActionStore, boolean instrumentationEnabled,
      DagProcessingEngine dagProcessingEngine, DagManagementStateStore dagManagementStateStore)
      throws IOException {
    this.config = config;
    Integer numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    this.dagActionStore = dagActionStore;
    this.scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    this.retentionPollingInterval = ConfigUtils.getInt(config, FAILED_DAG_POLLING_INTERVAL, DEFAULT_FAILED_DAG_POLLING_INTERVAL);
    this.eventBus = KafkaJobStatusMonitor.getEventBus();
    this.eventBus.register(this);
    this.dagProcessingEngine = dagProcessingEngine;
    this.dagManagementStateStore = dagManagementStateStore;
    MetricContext metricContext;
    if (instrumentationEnabled) {
      metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
      this.jobStatusPolledTimer = Optional.of(metricContext.timer(ServiceMetricNames.JOB_STATUS_POLLED_TIMER));
      this.eventSubmitter = Optional.of(new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build());
    } else {
      this.jobStatusPolledTimer = Optional.absent();
      this.eventSubmitter = Optional.absent();
    }
    this.dagManagerMetrics = new DagManagerMetrics();
    TimeUnit jobStartTimeUnit = TimeUnit.valueOf(ConfigUtils.getString(config, JOB_START_SLA_UNITS, ConfigurationKeys.FALLBACK_GOBBLIN_JOB_START_SLA_TIME_UNIT));
    this.defaultJobStartSlaTimeMillis = jobStartTimeUnit.toMillis(ConfigUtils.getLong(config, JOB_START_SLA_TIME, ConfigurationKeys.FALLBACK_GOBBLIN_JOB_START_SLA_TIME));
    this.jobStatusRetriever = jobStatusRetriever;
    this.quotaManager = GobblinConstructorUtils.invokeConstructor(UserQuotaManager.class,
        ConfigUtils.getString(config, ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER),
        config);
    TimeUnit timeUnit = TimeUnit.valueOf(ConfigUtils.getString(config, FAILED_DAG_RETENTION_TIME_UNIT, DEFAULT_FAILED_DAG_RETENTION_TIME_UNIT));
    this.failedDagRetentionTime = timeUnit.toMillis(ConfigUtils.getLong(config, FAILED_DAG_RETENTION_TIME, DEFAULT_FAILED_DAG_RETENTION_TIME));
    KillDagThread killDagThread = new KillDagThread(defaultJobStartSlaTimeMillis, this);
    this.scheduledExecutorPool.scheduleAtFixedRate(killDagThread, 100L, 60L, TimeUnit.SECONDS);
    this.failedDagStateStore = createDagStateStore(ConfigUtils.getConfigOrEmpty(config, FAILED_DAG_STATESTORE_PREFIX)
        .withFallback(config), topologySpecMap);
    setActive();
  }

  public synchronized void setActive() throws IOException {
    this.dagStateStore = createDagStateStore(config, topologySpecMap);
    this.failedDagIds = Collections.synchronizedSet(failedDagStateStore.getDagIds());
    this.dagManagerMetrics.activate();
    UserQuotaManager quotaManager = GobblinConstructorUtils.invokeConstructor(UserQuotaManager.class,
        ConfigUtils.getString(config, ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER), config);
    quotaManager.init(dagStateStore.getDags());
    DagManager.FailedDagRetentionThread
        failedDagRetentionThread = new DagManager.FailedDagRetentionThread(failedDagStateStore, failedDagIds, failedDagRetentionTime);
    this.scheduledExecutorPool.scheduleAtFixedRate(failedDagRetentionThread, 0, retentionPollingInterval, TimeUnit.MINUTES);
    loadDagFromDagStateStore();
    ScheduledExecutorService houseKeepingThreadPool = Executors.newSingleThreadScheduledExecutor();
    for (int delay = INITIAL_HOUSEKEEPING_THREAD_DELAY; delay < MAX_HOUSEKEEPING_THREAD_DELAY; delay *= 2) {
      houseKeepingThreadPool.schedule(() -> {
        try {
          loadDagFromDagStateStore();
        } catch (Exception e ) {
          log.error("failed to sync dag state store due to ", e);
        }}, delay, TimeUnit.MINUTES);
    }
  }

  public DagStateStore createDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
    try {
      Class<?> dagStateStoreClass = Class.forName(ConfigUtils.getString(config, DAG_STATESTORE_CLASS_KEY, FSDagStateStore.class.getName()));
      return (DagStateStore) GobblinConstructorUtils.invokeLongestConstructor(dagStateStoreClass, config, topologySpecMap);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addDag(String dagId, Dag<JobExecutionPlan> dag) {
    this.dagManagementStateStore.addDag(dagId, dag);
    this.dagManagementStateStore.addJobState(dagId, null);
  }

  @Override
  public Dag<JobExecutionPlan> getDag(String dagId) {
    return this.dagManagementStateStore.getDag(dagId);
  }

  @Override
  public boolean containsDag(String dagId) {
    return this.dagManagementStateStore.containsDag(dagId);
  }

  @Override
  public List<Dag.DagNode<JobExecutionPlan>> getAllJobs() throws IOException {
    return this.dagManagementStateStore.getAllJobs();
  }

  @Override
  public Map<String, Dag<JobExecutionPlan>> getResumingDags() {
    return null;
  }

  @Override
  public List<Dag.DagNode<JobExecutionPlan>> getJobs(String dagId) throws IOException {
    return this.dagManagementStateStore.getJobs(dagId);
  }

  @Override
  // todo - implement, should probably require dag id
  public Map<String, Long> getDagToSLA() {
    return null;
  }

  public void deleteJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    this.dagManagementStateStore.deleteJobState(dagId, dagNode);

  }

  public void addJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    this.dagManagementStateStore.addJobState(dagId, dagNode);
  }

  private void loadDagFromDagStateStore() throws IOException {
    List<Dag<JobExecutionPlan>> dags = this.dagStateStore.getDags();
    log.info("Loading " + dags.size() + " dags from dag state store");
    for (Dag<JobExecutionPlan> dag : dags) {
      // todo - reloaded dags should have a new type of FlowActionType
      this.dagProcessingEngine.addNewDag(dag);
    }
  }

  public void removeDagActionFromStore(DagActionStore.DagAction dagAction) throws IOException {
    if (this.dagActionStore.isPresent()) {
      this.dagActionStore.get().deleteDagAction(dagAction);
    }
  }

  //@Subscribe todo uncomment it when new dag manager is ready
  public void handleKillFlowEvent(KillFlowEvent killFlowEvent) {
    String flowGroup = killFlowEvent.getFlowGroup();
    String flowName = killFlowEvent.getFlowName();
    String flowExecutionId = String.valueOf(killFlowEvent.getFlowExecutionId());
    log.info("Received kill request for flow ({}, {}, {})", flowGroup, flowName, flowExecutionId);
    this.dagProcessingEngine.addDagAction(DagManagerUtils.createDagAction(flowGroup, flowName, flowExecutionId,
        DagActionStore.FlowActionType.KILL));
  }

  // @Subscribe todo uncomment it when new dag manager is ready
  public void handleResumeFlowEvent(ResumeFlowEvent resumeFlowEvent) {
    String flowGroup = resumeFlowEvent.getFlowGroup();
    String flowName = resumeFlowEvent.getFlowName();
    String flowExecutionId = String.valueOf(resumeFlowEvent.getFlowExecutionId());
    log.info("Received resume request for flow ({}, {}, {})", flowGroup, flowName, flowExecutionId);

    // todo - persist dag action instead of directly adding to the stream
    this.dagProcessingEngine.addDagAction(DagManagerUtils.createDagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.RESUME));
  }

  // @Subscribe todo uncomment it when new dag manager is ready
  public void handleJobStatusEvent(JobStatusEvent jobStatusEvent) {
    Map<String, Set<Dag.DagNode<JobExecutionPlan>>> nextSubmitted = Maps.newHashMap();
    List<Dag.DagNode<JobExecutionPlan>> nodesToCleanUp = Lists.newArrayList();

    ExecutionStatus executionStatus = jobStatusEvent.getStatus();
    JobStatus jobStatus = jobStatusEvent.getJobStatus();

    String dagNodeId = DagManagerUtils.generateDagNodeId(jobStatusEvent);
    Dag.DagNode<JobExecutionPlan> dagNode = this.dagManagementStateStore.getDagNode(dagNodeId);

    JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);

    try {
        switch (executionStatus) {
          case COMPLETE:
            jobExecutionPlan.setExecutionStatus(COMPLETE);
            nextSubmitted.putAll(onJobFinish(dagNode));
            nodesToCleanUp.add(dagNode);
            break;
          case FAILED:
            jobExecutionPlan.setExecutionStatus(FAILED);
            nextSubmitted.putAll(onJobFinish(dagNode));
            nodesToCleanUp.add(dagNode);
            break;
          case CANCELLED:
            jobExecutionPlan.setExecutionStatus(CANCELLED);
            nextSubmitted.putAll(onJobFinish(dagNode));
            nodesToCleanUp.add(dagNode);
            break;
          case PENDING:
            jobExecutionPlan.setExecutionStatus(PENDING);
            break;
          case PENDING_RETRY:
            jobExecutionPlan.setExecutionStatus(PENDING_RETRY);
            break;
          default:
            jobExecutionPlan.setExecutionStatus(RUNNING);
            break;
        }

        if (jobStatus != null && jobStatus.isShouldRetry()) {
          log.info("Retrying job: {}, current attempts: {}, max attempts: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode),
              jobStatus.getCurrentAttempts(), jobStatus.getMaxAttempts());
          this.dagManagementStateStore.getDagForJob(dagNode).setFlowEvent(null);
          this.dagProcessingEngine.addDagNodeToRetry(dagNode);
        }
      } catch (Exception e) {
        // Error occurred while processing dag, continue processing other dags assigned to this thread
        log.error(String.format("Exception caught in DagManager while processing dag %s due to ",
            DagManagerUtils.getFullyQualifiedDagName(dagNode)), e);
      }

    for (Map.Entry<String, Set<Dag.DagNode<JobExecutionPlan>>> entry: nextSubmitted.entrySet()) {
      String nextDagId = entry.getKey();
      Set<Dag.DagNode<JobExecutionPlan>> dagNodes = entry.getValue();
      for (Dag.DagNode<JobExecutionPlan> nextDagNode: dagNodes) {
        addJobState(nextDagId, nextDagNode);
      }
    }

    for (Dag.DagNode<JobExecutionPlan> dagNodeToClean: nodesToCleanUp) {
      String dagId = DagManagerUtils.generateDagId(dagNodeToClean).toString();
      deleteJobState(dagId, dagNodeToClean);
    }
  }

  /**
   * Method that defines the actions to be performed when a job finishes either successfully or with failure.
   * This method updates the state of the dag and performs clean up actions as necessary.
   */
  public Map<String, Set<Dag.DagNode<JobExecutionPlan>>> onJobFinish(Dag.DagNode<JobExecutionPlan> dagNode)
      throws IOException {
    Dag<JobExecutionPlan> dag = this.dagManagementStateStore.getDagForJob(dagNode);
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    String jobName = DagManagerUtils.getFullyQualifiedJobName(dagNode);
    ExecutionStatus jobStatus = DagManagerUtils.getExecutionStatus(dagNode);
    log.info("Job {} of Dag {} has finished with status {}", jobName, dagId, jobStatus.name());
    // Only decrement counters and quota for jobs that actually ran on the executor, not from a GaaS side failure/skip event
    if (quotaManager.releaseQuota(dagNode)) {
      dagManagerMetrics.decrementRunningJobMetrics(dagNode);
    }

    switch (jobStatus) {
      case FAILED:
        dag.setMessage("Flow failed because job " + jobName + " failed");
        dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_FAILED);
        dagManagerMetrics.incrementExecutorFailed(dagNode);
        return Maps.newHashMap();
      case CANCELLED:
        dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
        return Maps.newHashMap();
      case COMPLETE:
        dagManagerMetrics.incrementExecutorSuccess(dagNode);
        return submitNext(dagId);
      default:
        log.warn("It should not reach here. Job status is unexpected.");
        return Maps.newHashMap();
    }
  }

  /**
   * Submit next set of Dag nodes in the Dag identified by the provided dagId
   * @param dagId The dagId that should be processed.
   * @return
   * @throws IOException
   */
  private synchronized Map<String, Set<Dag.DagNode<JobExecutionPlan>>> submitNext(String dagId) throws IOException {
    Dag<JobExecutionPlan> dag = this.dagManagementStateStore.getDag(dagId);
    Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);
    List<String> nextJobNames = new ArrayList<>();

    //Submit jobs from the dag ready for execution.
    for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
      this.dagProcessingEngine.addAdvanceDagAction(dagNode);
      nextJobNames.add(DagManagerUtils.getJobName(dagNode));
    }
    log.info("Submitting next nodes for dagId {}, where next jobs to be submitted are {}", dagId, nextJobNames);
    //Checkpoint the dag state
    this.dagStateStore.writeCheckpoint(dag);

    Map<String, Set<Dag.DagNode<JobExecutionPlan>>> dagIdToNextJobs = Maps.newHashMap();
    dagIdToNextJobs.put(dagId, nextNodes);
    return dagIdToNextJobs;
  }
}
