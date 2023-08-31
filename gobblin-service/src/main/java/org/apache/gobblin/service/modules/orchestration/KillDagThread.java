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
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;
import static org.apache.gobblin.service.ExecutionStatus.ORCHESTRATED;
import static org.apache.gobblin.service.ExecutionStatus.valueOf;


/**
 * A thread that checks all the jobs (dag nodes) and kill them if the passed job start or job completion deadlines.
 */
@Slf4j
public class KillDagThread implements Runnable {
  private final Long defaultJobStartSlaTimeMillis;
  NewDagManager dagManager;
  public KillDagThread(Long defaultJobStartSlaTimeMillis, NewDagManager newDagManager) {
    this.defaultJobStartSlaTimeMillis = defaultJobStartSlaTimeMillis;
    this.dagManager = newDagManager;
  }

  @Override
  public void run() {
    try {
      for (Dag.DagNode<JobExecutionPlan> node : this.dagManager.getAllJobs()) {
        boolean flowKilled = enforceFlowStartDeadline(node);
        boolean jobKilled = false;

        if (!flowKilled) {
          JobStatus jobStatus = pollJobStatus(node);
          try {
            jobKilled = enforceJobCompletionDeadline(node, jobStatus);
          } catch (ExecutionException | InterruptedException e) {
            log.warn("Error getting status for dag node " + node.getId());
            continue;
          }
        }

        if (flowKilled || jobKilled) {
          JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(node);
          jobExecutionPlan.setExecutionStatus(CANCELLED);
          try {
            // It is likely that the job killed event will not be emitted by spec producer, so call onJobFinish
            this.dagManager.onJobFinish(node);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          String dagId = DagManagerUtils.generateDagId(node).toString();
          this.dagManager.deleteJobState(dagId, node);
        }
      }
    } catch (IOException e) {
      log.warn("Failed to get all jobs from the dag management store.");
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve the {@link JobStatus} from the {@link JobExecutionPlan}.
   */
  private JobStatus pollJobStatus(Dag.DagNode<JobExecutionPlan> dagNode) {
    Config jobConfig = dagNode.getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    String jobGroup = jobConfig.getString(ConfigurationKeys.JOB_GROUP_KEY);
    String jobName = jobConfig.getString(ConfigurationKeys.JOB_NAME_KEY);

    return pollStatus(flowGroup, flowName, flowExecutionId, jobGroup, jobName);
  }

  private JobStatus pollStatus(String flowGroup, String flowName, long flowExecutionId, String jobGroup, String jobName) {
    long pollStartTime = System.nanoTime();
    Iterator<JobStatus> jobStatusIterator =
        this.dagManager.getJobStatusRetriever().getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId, jobName, jobGroup);
    Instrumented.updateTimer(this.dagManager.getJobStatusPolledTimer(), System.nanoTime() - pollStartTime, TimeUnit.NANOSECONDS);

    if (jobStatusIterator.hasNext()) {
      return jobStatusIterator.next();
    } else {
      return null;
    }
  }

  private boolean enforceJobCompletionDeadline(Dag.DagNode<JobExecutionPlan> node, JobStatus jobStatus)
      throws ExecutionException, InterruptedException {
    if (jobStatus == null) {
      return false;
    }
    ExecutionStatus executionStatus = valueOf(jobStatus.getEventName());
    long timeOutForJobStart = DagManagerUtils.getJobStartSla(node, this.defaultJobStartSlaTimeMillis);
    long jobOrchestratedTime = jobStatus.getOrchestratedTime();
    if (executionStatus == ORCHESTRATED && System.currentTimeMillis() - jobOrchestratedTime > timeOutForJobStart) {
      log.info("Job {} of flow {} exceeded the job start SLA of {} ms. Killing the job now...",
          DagManagerUtils.getJobName(node),
          DagManagerUtils.getFullyQualifiedDagName(node),
          timeOutForJobStart);
      this.dagManager.getDagManagerMetrics().incrementCountsStartSlaExceeded(node);
      this.dagManager.getDagProcessingEngine().addKillDagAction(node);
      String dagId = DagManagerUtils.generateDagId(node).toString();
      this.dagManager.getDag(dagId).setFlowEvent(TimingEvent.FlowTimings.FLOW_START_DEADLINE_EXCEEDED);
      this.dagManager.getDag(dagId).setMessage("Flow killed because no update received for " + timeOutForJobStart + " ms after orchestration");
      return true;
    } else {
      return false;
    }
  }

  private boolean enforceFlowStartDeadline(Dag.DagNode<JobExecutionPlan> node) {
    long flowStartTime = DagManagerUtils.getFlowStartTime(node);
    long currentTime = System.currentTimeMillis();
    String dagId = DagManagerUtils.generateDagId(node).toString();

    long flowSla;
    if (this.dagManager.getDagToSLA().containsKey(dagId)) {
      flowSla = this.dagManager.getDagToSLA().get(dagId);
    } else {
      try {
        flowSla = DagManagerUtils.getFlowSLA(node);
      } catch (ConfigException e) {
        log.warn("Flow SLA for flowGroup: {}, flowName: {} is given in invalid format, using default SLA of {}",
            node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY),
            node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY),
            DagManagerUtils.DEFAULT_FLOW_SLA_MILLIS);
        flowSla = DagManagerUtils.DEFAULT_FLOW_SLA_MILLIS;
      }
      this.dagManager.getDagToSLA().put(dagId, flowSla);
    }

    if (currentTime > flowStartTime + flowSla) {
      log.info("Flow {} exceeded the SLA of {} ms. Killing the job {} now...",
          node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), flowSla,
          node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY));
      this.dagManager.getDagManagerMetrics().incrementExecutorSlaExceeded(node);
      this.dagManager.getDagProcessingEngine().addKillDagAction(node);

      this.dagManager.getDag(dagId).setFlowEvent(TimingEvent.FlowTimings.FLOW_RUN_DEADLINE_EXCEEDED);
      this.dagManager.getDag(dagId).setMessage("Flow killed due to exceeding SLA of " + flowSla + " ms");

      return true;
    }
    return false;
  }
}
