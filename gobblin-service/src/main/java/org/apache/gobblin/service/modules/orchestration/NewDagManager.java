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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ReloadDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.modules.utils.SharedFlowMetricsSingleton;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
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
 * NewDagManager manages dags in memory and various mappings.
 */
@Slf4j
public class NewDagManager implements DagManagement {
  public static final String DAG_MANAGER_PREFIX = "gobblin.service.dagManager.";

  private static final Integer DEFAULT_JOB_STATUS_POLLING_INTERVAL = 10;
  public static final Integer DEFAULT_NUM_THREADS = 3;
  private static final Integer TERMINATION_TIMEOUT = 30;
  public static final String NUM_THREADS_KEY = DAG_MANAGER_PREFIX + "numThreads";
  public static final String JOB_STATUS_POLLING_INTERVAL_KEY = DAG_MANAGER_PREFIX + "pollingInterval";
  private static final String DAG_STATESTORE_CLASS_KEY = DAG_MANAGER_PREFIX + "dagStateStoreClass";
  private static final String FAILED_DAG_STATESTORE_PREFIX = "failedDagStateStore";
  private static final String FAILED_DAG_RETENTION_TIME_UNIT = FAILED_DAG_STATESTORE_PREFIX + ".retention.timeUnit";
  private static final String DEFAULT_FAILED_DAG_RETENTION_TIME_UNIT = "DAYS";
  private static final String FAILED_DAG_RETENTION_TIME = FAILED_DAG_STATESTORE_PREFIX + ".retention.time";
  private static final long DEFAULT_FAILED_DAG_RETENTION_TIME = 7L;
  // Re-emit the final flow status if not detected within 5 minutes
  public static final String FAILED_DAG_POLLING_INTERVAL = FAILED_DAG_STATESTORE_PREFIX + ".retention.pollingIntervalMinutes";
  public static final Integer DEFAULT_FAILED_DAG_POLLING_INTERVAL = 60;
  public static final String DAG_MANAGER_HEARTBEAT = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX_WITH_DELIMITER + "dagManager.heartbeat-%s";
  // Default job start SLA time if configured, measured in minutes. Default is 10 minutes
  private static final String JOB_START_SLA_TIME = DAG_MANAGER_PREFIX + ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME;
  private static final String JOB_START_SLA_UNITS = DAG_MANAGER_PREFIX + ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME_UNIT;
  private static final int INITIAL_HOUSEKEEPING_THREAD_DELAY = 2;
  private final Config config;
  private final Integer retentionPollingInterval;

  public static String getFAILED_DAG_STATESTORE_PREFIX() {
    return NewDagManager.FAILED_DAG_STATESTORE_PREFIX;
  }

  public void addDag(String dagId, Dag<JobExecutionPlan> dag) {
    // TODO : implement it, get some code from old dag manager
  }


  public Map<String, Dag<JobExecutionPlan>> getDags() {
    return this.dags;
  }

  public Map<String, Dag<JobExecutionPlan>> getResumingDags() {
    return this.resumingDags;
  }

  public Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> getJobToDag() {
    return this.jobToDag;
  }

  public Map<String, Dag.DagNode<JobExecutionPlan>> getDagNodes() {
    return this.dagNodes;
  }

  public Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> getDagToJobs() {
    return this.dagToJobs;
  }

  public Map<String, Long> getDagToSLA() {
    return this.dagToSLA;
  }

  public Set<String> getFailedDagIds() {
    return this.failedDagIds;
  }

  public DagStateStore getFailedDagStateStore() {
    return this.failedDagStateStore;
  }

  public DagStateStore getDagStateStore() {
    return this.dagStateStore;
  }

  public Integer getNumThreads() {
    return this.numThreads;
  }

  public JobStatusRetriever getJobStatusRetriever() {
    return this.jobStatusRetriever;
  }

  public UserQuotaManager getQuotaManager() {
    return this.quotaManager;
  }

  public Optional<Timer> getJobStatusPolledTimer() {
    return this.jobStatusPolledTimer;
  }

  public Optional<EventSubmitter> getEventSubmitter() {
    return this.eventSubmitter;
  }

  public DagManagerMetrics getDagManagerMetrics() {
    return this.dagManagerMetrics;
  }

  public AtomicLong getOrchestrationDelay() {
    return this.orchestrationDelay;
  }

  public DagProcessingEngine getDagProcessingEngine() {
    return this.dagProcessingEngine;
  }

  public Optional<DagActionStore> getDagActionStore() {
    return this.dagActionStore;
  }

  /**
   * Action to be performed on a {@link Dag}, in case of a job failure. Currently, we allow 2 modes:
   * <ul>
   *   <li> FINISH_RUNNING, which allows currently running jobs to finish.</li>
   *   <li> FINISH_ALL_POSSIBLE, which allows every possible job in the Dag to finish, as long as all the dependencies
   *   of the job are successful.</li>
   * </ul>
   */
  public enum FailureOption {
    FINISH_RUNNING("FINISH_RUNNING"),
    CANCEL("CANCEL"),
    FINISH_ALL_POSSIBLE("FINISH_ALL_POSSIBLE");

    private final String failureOption;

    FailureOption(final String failureOption) {
      this.failureOption = failureOption;
    }

    @Override
    public String toString() {
      return this.failureOption;
    }
  }

  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor
  public static class DagId {
    String flowGroup;
    String flowName;
    long flowExecutionId;

    @Override
    public String toString() {
      return Joiner.on("_").join(flowGroup, flowName, flowExecutionId);
    }
  }

  private final Map<String, Dag<JobExecutionPlan>> dags = new HashMap<>();
  private final Map<String, Dag<JobExecutionPlan>> resumingDags = new HashMap<>();
  private static final long DAG_FLOW_STATUS_TOLERANCE_TIME_MILLIS = TimeUnit.MINUTES.toMillis(5);


  private final Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> jobToDag = new HashMap<>();
  private final Map<String, Dag.DagNode<JobExecutionPlan>> dagNodes = new HashMap<>();
  private final Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> dagToJobs = new HashMap<>();
  final Map<String, Long> dagToSLA = new HashMap<>();
  DagManager.DagManagerThread[] dagManagerThreads;

  private final ScheduledExecutorService scheduledExecutorPool;
  private Set<String> failedDagIds;
  private final DagStateStore failedDagStateStore;

  private Map<URI, TopologySpec> topologySpecMap = new HashMap<>();
  private final boolean instrumentationEnabled;
  private DagStateStore dagStateStore;
  private int houseKeepingThreadInitialDelay = INITIAL_HOUSEKEEPING_THREAD_DELAY;
  private final Integer numThreads;
  protected final Long defaultJobStartSlaTimeMillis;
  private final JobStatusRetriever jobStatusRetriever;
  private final UserQuotaManager quotaManager;
  private final SpecCompiler specCompiler;
  private final boolean isFlowConcurrencyEnabled;
  private final FlowCompilationValidationHelper flowCompilationValidationHelper;
  private final Optional<Timer> jobStatusPolledTimer;
  private final Optional<EventSubmitter> eventSubmitter;
  // todo implement
  private final long failedDagRetentionTime;
  private final DagManagerMetrics dagManagerMetrics;
  private final AtomicLong orchestrationDelay = new AtomicLong(0);
  private final DagProcessingEngine dagProcessingEngine;


  @Inject(optional=true)
  protected Optional<DagActionStore> dagActionStore;
  private static final int MAX_HOUSEKEEPING_THREAD_DELAY = 180;

  protected final EventBus eventBus;
  private final FlowCatalog flowCatalog;

  public NewDagManager(Config config, JobStatusRetriever jobStatusRetriever,
      SharedFlowMetricsSingleton sharedFlowMetricsSingleton, FlowStatusGenerator flowStatusGenerator,
      FlowCatalog flowCatalog, Optional<DagActionStore> dagActionStore, boolean instrumentationEnabled, DagProcessingEngine dagProcessingEngine)
      throws IOException {
    this.config = config;
    this.numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    this.dagActionStore = dagActionStore;
    this.scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    this.retentionPollingInterval = ConfigUtils.getInt(config, FAILED_DAG_POLLING_INTERVAL, DEFAULT_FAILED_DAG_POLLING_INTERVAL);
    this.instrumentationEnabled = instrumentationEnabled;
    this.eventBus = KafkaJobStatusMonitor.getEventBus();
    this.eventBus.register(this);
    this.dagProcessingEngine = dagProcessingEngine;
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
    this.specCompiler = GobblinConstructorUtils.invokeConstructor(SpecCompiler.class, ConfigUtils.getString(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY,
        ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS), config);
    this.isFlowConcurrencyEnabled = ConfigUtils.getBoolean(config, ServiceConfigKeys.FLOW_CONCURRENCY_ALLOWED,
        ServiceConfigKeys.DEFAULT_FLOW_CONCURRENCY_ALLOWED);
    this.quotaManager = GobblinConstructorUtils.invokeConstructor(UserQuotaManager.class,
        ConfigUtils.getString(config, ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER),
        config);
    this.flowCompilationValidationHelper = new FlowCompilationValidationHelper(sharedFlowMetricsSingleton, specCompiler,
        quotaManager, eventSubmitter, flowStatusGenerator, isFlowConcurrencyEnabled);
    TimeUnit timeUnit = TimeUnit.valueOf(ConfigUtils.getString(config, FAILED_DAG_RETENTION_TIME_UNIT, DEFAULT_FAILED_DAG_RETENTION_TIME_UNIT));
    this.failedDagRetentionTime = timeUnit.toMillis(ConfigUtils.getLong(config, FAILED_DAG_RETENTION_TIME, DEFAULT_FAILED_DAG_RETENTION_TIME));
    KillDagThread killDagThread = new KillDagThread(defaultJobStartSlaTimeMillis, this);
    this.scheduledExecutorPool.scheduleAtFixedRate(killDagThread, 100L, 60L, TimeUnit.SECONDS);
    this.flowCatalog = flowCatalog;
    this.failedDagStateStore =
        createDagStateStore(ConfigUtils.getConfigOrEmpty(config, FAILED_DAG_STATESTORE_PREFIX).withFallback(config),
            topologySpecMap);
    setActive();
  }

  public synchronized void setActive() throws IOException {
    this.dagStateStore = createDagStateStore(config, topologySpecMap);
    DagStateStore failedDagStateStore =
        createDagStateStore(ConfigUtils.getConfigOrEmpty(config, FAILED_DAG_STATESTORE_PREFIX).withFallback(config),
            topologySpecMap);
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
    for (int delay = houseKeepingThreadInitialDelay; delay < MAX_HOUSEKEEPING_THREAD_DELAY; delay *= 2) {
      houseKeepingThreadPool.schedule(() -> {
        try {
          loadDagFromDagStateStore();
        } catch (Exception e ) {
          log.error("failed to sync dag state store due to ", e);
        }}, delay, TimeUnit.MINUTES);
    }
    if (dagActionStore.isPresent()) {
      Collection<DagActionStore.DagAction> dagActions = dagActionStore.get().getDagActions();
      for (DagActionStore.DagAction action : dagActions) {
        switch (action.getFlowActionType()) {
          case KILL:
            this.handleKillFlowEvent(new KillFlowEvent(action.getFlowGroup(), action.getFlowName(), Long.parseLong(action.getFlowExecutionId())));
            break;
          case RESUME:
            this.handleResumeFlowEvent(new ResumeFlowEvent(action.getFlowGroup(), action.getFlowName(), Long.parseLong(action.getFlowExecutionId())));
            break;
          case LAUNCH:
            this.handleLaunchFlowEvent(action);
            break;
          default:
            log.warn("Unsupported dagAction: " + action.getFlowActionType().toString());
        }
      }
    }
  }

  private void loadDagFromDagStateStore() throws IOException {
    List<Dag<JobExecutionPlan>> dags = this.dagStateStore.getDags();
    log.info("Loading " + dags.size() + " dags from dag state store");
    for (Dag<JobExecutionPlan> dag : dags) {
      this.dagProcessingEngine.addDagTask(new ReloadDagTask(dag));
    }
  }

  // todo call it from orchestrator
  public synchronized void setTopologySpecMap(Map<URI, TopologySpec> topologySpecMap) {
    this.topologySpecMap = topologySpecMap;
  }

  public DagStateStore createDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
    try {
      Class<?> dagStateStoreClass = Class.forName(ConfigUtils.getString(config, DAG_STATESTORE_CLASS_KEY, FSDagStateStore.class.getName()));
      return (DagStateStore) GobblinConstructorUtils.invokeLongestConstructor(dagStateStoreClass, config, topologySpecMap);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  //@Subscribe todo uncomment it when new dag manager is ready
  public void handleKillFlowEvent(KillFlowEvent killFlowEvent) {
    String flowGroup = killFlowEvent.getFlowGroup();
    String flowName = killFlowEvent.getFlowName();
    long flowExecutionId = killFlowEvent.getFlowExecutionId();
    log.info("Received kill request for flow ({}, {}, {})", flowGroup, flowName, flowExecutionId);
    DagId dagId = new DagId(flowGroup, flowName, flowExecutionId);
    this.dagProcessingEngine.addDagTask(new KillDagTask(dagId));
  }

  public void handleLaunchFlowEvent(DagActionStore.DagAction launchAction) {
    Preconditions.checkArgument(launchAction.getFlowActionType() == DagActionStore.FlowActionType.LAUNCH);
    log.info("Handle launch flow event for action {}", launchAction);
    FlowId flowId = launchAction.getFlowId();
    try {
      URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);
      FlowSpec spec = (FlowSpec) flowCatalog.getSpecs(flowUri);
      Optional<Dag<JobExecutionPlan>> optionalJobExecutionPlanDag =
          this.flowCompilationValidationHelper.createExecutionPlanIfValid(spec,
              Optional.of(spec.getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)));
      if (optionalJobExecutionPlanDag.isPresent()) {
        this.dagProcessingEngine.addNewDag(optionalJobExecutionPlanDag.get());
      }
      // Upon handling the action, delete it so on leadership change this is not duplicated
      this.dagActionStore.get().deleteDagAction(launchAction);
    } catch (URISyntaxException e) {
      log.warn(String.format("Could not create URI object for flowId %s due to exception", flowId), e);
      this.dagManagerMetrics.incrementFailedLaunchCount();
    } catch (SpecNotFoundException e) {
      log.warn(String.format("Spec not found for flowId %s due to exception", flowId), e);
      this.dagManagerMetrics.incrementFailedLaunchCount();
    } catch (IOException e) {
      log.warn(String.format("Failed to add Job Execution Plan for flowId %s OR delete dag action from dagActionStore "
          + "(check stacktrace) due to exception", flowId), e);
      this.dagManagerMetrics.incrementFailedLaunchCount();
    } catch (InterruptedException e) {
      log.warn(String.format("SpecCompiler failed to reach healthy state before compilation of flowId %s due to "
          + "exception", flowId), e);
      this.dagManagerMetrics.incrementFailedLaunchCount();
    }
  }

  // @Subscribe todo uncomment it when new dag manager is ready
  public void handleResumeFlowEvent(ResumeFlowEvent resumeFlowEvent) throws IOException {
    String flowGroup = resumeFlowEvent.getFlowGroup();
    String flowName = resumeFlowEvent.getFlowName();
    long flowExecutionId = resumeFlowEvent.getFlowExecutionId();
    log.info("Received resume request for flow ({}, {}, {})", flowGroup, flowName, flowExecutionId);
    DagId dagIdToResume = new DagId(flowGroup, flowName, flowExecutionId);

    this.dagProcessingEngine.addDagTask(new ResumeDagTask(dagIdToResume));
  }

  public void removeDagActionFromStore(DagId dagIdToResume, DagActionStore.FlowActionType flowActionType)
      throws IOException {
    if (this.dagActionStore.isPresent()) {
      this.dagActionStore.get().deleteDagAction(new DagActionStore.DagAction(
          dagIdToResume.flowGroup, dagIdToResume.flowName, String.valueOf(dagIdToResume.flowExecutionId), flowActionType));
    }
  }

  // @Subscribe todo uncomment it when new dag manager is ready
  public void handleJobStatusEvent(JobStatusEvent jobStatusEvent) {
    Map<String, Set<Dag.DagNode<JobExecutionPlan>>> nextSubmitted = Maps.newHashMap();
    List<Dag.DagNode<JobExecutionPlan>> nodesToCleanUp = Lists.newArrayList();

    ExecutionStatus executionStatus = jobStatusEvent.getStatus();
    JobStatus jobStatus = jobStatusEvent.getJobStatus();

    String dagNodeId = DagManagerUtils.generateDagNodeId(jobStatusEvent).toString();
    Dag.DagNode<JobExecutionPlan> dagNode = this.dagNodes.get(dagNodeId);

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
          this.jobToDag.get(dagNode).setFlowEvent(null);

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

  public void deleteJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    this.jobToDag.remove(dagNode);
    this.dagToJobs.get(dagId).remove(dagNode);
    this.dagToSLA.remove(dagId);
  }

  public void addJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    Dag<JobExecutionPlan> dag = this.dags.get(dagId);
    this.jobToDag.put(dagNode, dag);
    if (this.dagToJobs.containsKey(dagId)) {
      this.dagToJobs.get(dagId).add(dagNode);
    } else {
      LinkedList<Dag.DagNode<JobExecutionPlan>> dagNodeList = Lists.newLinkedList();
      dagNodeList.add(dagNode);
      this.dagToJobs.put(dagId, dagNodeList);
    }
  }

  /**
   * Method that defines the actions to be performed when a job finishes either successfully or with failure.
   * This method updates the state of the dag and performs clean up actions as necessary.
   */
  public Map<String, Set<Dag.DagNode<JobExecutionPlan>>> onJobFinish(Dag.DagNode<JobExecutionPlan> dagNode)
      throws IOException {
    Dag<JobExecutionPlan> dag = this.jobToDag.get(dagNode);
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
  public synchronized Map<String, Set<Dag.DagNode<JobExecutionPlan>>> submitNext(String dagId) throws IOException {
    Dag<JobExecutionPlan> dag = this.dags.get(dagId);
    Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);
    List<String> nextJobNames = new ArrayList<>();

    //Submit jobs from the dag ready for execution.
    for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
      this.dagProcessingEngine.addAdvanceDagTask(dagNode);
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
