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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.RetryDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.RUNNING;


/**
 * An implementation of {@link DagProc} for launching {@link org.apache.gobblin.service.modules.orchestration.task.DagTask}.
 */
@Slf4j
@Alpha
public final class RetryDagProc extends DagProc<RetryDagTask> {
  private RetryDagTask retryDagTask;
  private Optional<Dag.DagNode<JobExecutionPlan>> dagNodeToRetry;

  public RetryDagProc(RetryDagTask retryDagTask, DagProcessingEngine dagProcFactory) {
    super(dagProcFactory);
    this.retryDagTask = retryDagTask;
  }

  @Override
  protected void initialize() throws IOException {
    this.dagNodeToRetry = null; // todo - get dag node
  }

  @Override
  protected void act() {
    submitJob(this.dagNodeToRetry.get());
  }

  /**
   * Submits a {@link JobSpec} to a {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   */
  private void submitJob(Dag.DagNode<JobExecutionPlan> dagNode) {
    // de duplicate it with LaunchDagProc
      DagManagerUtils.incrementJobAttempt(dagNode);
      JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
      jobExecutionPlan.setExecutionStatus(RUNNING);
      JobSpec jobSpec = DagManagerUtils.getJobSpec(dagNode);
      Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);

      String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);

      // Run this spec on selected executor
      SpecProducer<Spec> producer;
      try {
        quotaManager.checkQuota(Collections.singleton(dagNode));

        producer = DagManagerUtils.getSpecProducer(dagNode);
        TimingEvent jobOrchestrationTimer = this.eventSubmitter.isPresent() ? this.eventSubmitter.get().
            getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED) : null;

        // Increment job count before submitting the job onto the spec producer, in case that throws an exception.
        // By this point the quota is allocated, so it's imperative to increment as missing would introduce the potential to decrement below zero upon quota release.
        // Quota release is guaranteed, despite failure, because exception handling within would mark the job FAILED.
        // When the ensuing kafka message spurs DagManager processing, the quota is released and the counts decremented
        // Ensure that we do not double increment for flows that are retried
        if (dagNode.getValue().getCurrentAttempts() == 1) {
          dagManagerMetrics.incrementRunningJobMetrics(dagNode);
        }
        // Submit the job to the SpecProducer, which in turn performs the actual job submission to the SpecExecutor instance.
        // The SpecProducer implementations submit the job to the underlying executor and return when the submission is complete,
        // either successfully or unsuccessfully. To catch any exceptions in the job submission, the DagManagerThread
        // blocks (by calling Future#get()) until the submission is completed.
        Future<?> addSpecFuture = producer.addSpec(jobSpec);
        dagNode.getValue().setJobFuture(Optional.of(addSpecFuture));
        //Persist the dag
        this.dagStateStore.writeCheckpoint(this.dagManager.getDag(DagManagerUtils.generateDagId(dagNode).toString()));

        addSpecFuture.get();

        jobMetadata.put(TimingEvent.METADATA_MESSAGE, producer.getExecutionLink(addSpecFuture, specExecutorUri));
        // Add serialized job properties as part of the orchestrated job event metadata
        jobMetadata.put(JobExecutionPlan.JOB_PROPS_KEY, dagNode.getValue().toString());
        if (jobOrchestrationTimer != null) {
          jobOrchestrationTimer.stop(jobMetadata);
        }
        log.info("Orchestrated job: {} on Executor: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode), specExecutorUri);
        this.dagManagerMetrics.incrementJobsSentToExecutor(dagNode);
      } catch (Exception e) {
        TimingEvent jobFailedTimer = this.eventSubmitter.isPresent() ? this.eventSubmitter.get().
            getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED) : null;
        String message = "Cannot submit job " + DagManagerUtils.getFullyQualifiedJobName(dagNode) + " on executor " + specExecutorUri;
        log.error(message, e);
        jobMetadata.put(TimingEvent.METADATA_MESSAGE, message + " due to " + e.getMessage());
        if (jobFailedTimer != null) {
          jobFailedTimer.stop(jobMetadata);
        }
      }
    }
}
