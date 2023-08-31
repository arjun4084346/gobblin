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

package org.apache.gobblin.service.modules.orchestration.proc;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementTaskStreamImpl;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;


/**
 * An implementation for {@link LaunchDagTask}
 */
@Slf4j
@Alpha
public class LaunchDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>, Optional<Dag<JobExecutionPlan>>> {
  private final LaunchDagTask launchDagTask;
  FlowCompilationValidationHelper flowCompilationValidationHelper;

  public LaunchDagProc(LaunchDagTask launchDagTask, FlowCompilationValidationHelper flowCompilationValidationHelper) {
    this.launchDagTask = launchDagTask;
    AtomicLong orchestrationDelayCounter = new AtomicLong(0);
    ContextAwareGauge<Long> orchestrationDelayMetric = metricContext.newContextAwareGauge
        (ServiceMetricNames.FLOW_ORCHESTRATION_DELAY, orchestrationDelayCounter::get);
    metricContext.register(orchestrationDelayMetric);
    this.flowCompilationValidationHelper = flowCompilationValidationHelper;
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    try {
      DagActionStore.DagAction dagAction = this.launchDagTask.getDagAction();
      URI flowUri = FlowSpec.Utils.createFlowSpecUri(dagAction.getFlowId());
      FlowSpec flowSpec = dagManagementStateStore.getFlowSpec(flowUri);
      flowSpec.addProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, dagAction.getFlowExecutionId());
      return this.flowCompilationValidationHelper.createExecutionPlanIfValid(flowSpec).toJavaUtil();
    } catch (URISyntaxException | SpecNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    if (!dag.isPresent()) {
      log.warn("No dag with id " + this.launchDagTask.getDagId() + " found to launch");
      return Optional.empty();
    }
    DagManager.DagId dagId = DagManagerUtils.generateDagId(dag.get());
    Set<Dag.DagNode<JobExecutionPlan>> nextSubmitted = submitNext(dagManagementStateStore, dag.get());
    for (Dag.DagNode<JobExecutionPlan> dagNode : nextSubmitted) {
      dagManagementStateStore.addDagNodeState(dagNode, dagId);  // compare this - arjun1
    }

    log.info("Dag {} processed.", dagId);
    return dag;
  }

  /**
   * Submit next set of Dag nodes in the Dag identified by the provided dagId
   */
   private Set<Dag.DagNode<JobExecutionPlan>> submitNext(DagManagementStateStore dagManagementStateStore,
       Dag<JobExecutionPlan> dag) throws IOException {
     DagManager.DagId dagId = DagManagerUtils.generateDagId(dag);
     Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);
     List<String> nextJobNames = new ArrayList<>();

     //Submit jobs from the dag ready for execution.
     for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
       submitJob(dagManagementStateStore, dagNode);
       nextJobNames.add(DagManagerUtils.getJobName(dagNode));
     }

     log.info("Submitting next nodes for dagId {}, where next jobs to be submitted are {}", dagId, nextJobNames);

     //Checkpoint the dag state, it should have an updated value of dag nodes
     dagManagementStateStore.checkpointDag(dag);

     return nextNodes;
  }

  /**
   * Submits a {@link JobSpec} to a {@link SpecExecutor}.
   */
  private void submitJob(DagManagementStateStore dagManagementStateStore, Dag.DagNode<JobExecutionPlan> dagNode) {
    DagManagerUtils.incrementJobAttempt(dagNode);
    JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
    jobExecutionPlan.setExecutionStatus(ExecutionStatus.RUNNING);
    JobSpec jobSpec = DagManagerUtils.getJobSpec(dagNode);
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);

    String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);

    // Run this spec on selected executor
    SpecProducer<Spec> producer;
    try {
      dagManagementStateStore.tryAcquireQuota(Collections.singleton(dagNode));
      producer = DagManagerUtils.getSpecProducer(dagNode);
      TimingEvent jobOrchestrationTimer = eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED);

      // Increment job count before submitting the job onto the spec producer, in case that throws an exception.
      // By this point the quota is allocated, so it's imperative to increment as missing would introduce the potential to decrement below zero upon quota release.
      // Quota release is guaranteed, despite failure, because exception handling within would mark the job FAILED.
      // When the ensuing kafka message spurs DagManager processing, the quota is released and the counts decremented
      // Ensure that we do not double increment for flows that are retried
      if (dagNode.getValue().getCurrentAttempts() == 1) {
        DagManagementTaskStreamImpl.getDagManagerMetrics().incrementRunningJobMetrics(dagNode);
      }
      // Submit the job to the SpecProducer, which in turn performs the actual job submission to the SpecExecutor instance.
      // The SpecProducer implementations submit the job to the underlying executor and return when the submission is complete,
      // either successfully or unsuccessfully. To catch any exceptions in the job submission, the DagManagerThread
      // blocks (by calling Future#get()) until the submission is completed.
      Future<?> addSpecFuture = producer.addSpec(jobSpec);
      dagNode.getValue().setJobFuture(com.google.common.base.Optional.of(addSpecFuture));

      addSpecFuture.get();

      jobMetadata.put(TimingEvent.METADATA_MESSAGE, producer.getExecutionLink(addSpecFuture, specExecutorUri));
      // Add serialized job properties as part of the orchestrated job event metadata
      jobMetadata.put(JobExecutionPlan.JOB_PROPS_KEY, dagNode.getValue().toString());
      jobOrchestrationTimer.stop(jobMetadata);
      log.info("Orchestrated job: {} on Executor: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode), specExecutorUri);
      DagManagementTaskStreamImpl.getDagManagerMetrics().incrementJobsSentToExecutor(dagNode);
    } catch (Exception e) {
      TimingEvent jobFailedTimer = eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED);
      String message = "Cannot submit job " + DagManagerUtils.getFullyQualifiedJobName(dagNode) + " on executor " + specExecutorUri;
      log.error(message, e);
      jobMetadata.put(TimingEvent.METADATA_MESSAGE, message + " due to " + e.getMessage());
      if (jobFailedTimer != null) {
        jobFailedTimer.stop(jobMetadata);
      }
    }
  }

  @Override
  protected void sendNotification(Optional<Dag<JobExecutionPlan>> result, EventSubmitter eventSubmitter) {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
