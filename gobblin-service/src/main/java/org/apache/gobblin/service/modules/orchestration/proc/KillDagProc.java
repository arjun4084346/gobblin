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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcFactory;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;


/**
 * An implementation of {@link DagProc} for killing {@link DagTask}.
 */
@Slf4j
@Alpha
public final class KillDagProc extends DagProc<KillDagTask> {

  private KillDagTask killDagTask;

  public KillDagProc(KillDagTask killDagTask, DagProcFactory dagProcFactory) {
    super(dagProcFactory);
    this.killDagTask = killDagTask;
  }

  @Override
  public void process(KillDagTask dagTask) throws IOException {
    if (dagTask.getDagId() != null) {
      String dagId = dagTask.getDagId().toString();
      killByDagId(dagId);
    }
    if (dagTask.getDagNodeId() != null) {
      String dagNodeId = dagTask.getDagNodeId().toString();
      killByDagNodeId(dagNodeId);
    }
    if (dagTask.getUri() != null) {
      URI uri = dagTask.getUri();
      killByUri(uri);
    }
  }

  private void killByUri(URI uri) throws IOException {
    String flowGroup = FlowSpec.Utils.getFlowGroup(uri);
    String flowName = FlowSpec.Utils.getFlowName(uri);
    List<Long> flowExecutionIds = this.dagManager.getJobStatusRetriever().getLatestExecutionIdsForFlow
        (flowName, flowGroup, 10);
    log.info("Found {} flows to cancel.", flowExecutionIds.size());
    for (long flowExecutionId : flowExecutionIds) {
      killByDagId(DagManagerUtils.generateDagId(flowGroup, flowName, flowExecutionId).toString());
    }
  }

  private void killByDagNodeId(String dagNodeId) throws IOException {
    if (this.dagManager.getDagNodes().containsKey(dagNodeId)) {
      cancelDagNode(this.dagManager.getDagNodes().get(dagNodeId));
    } else {
      log.warn("Dag node to cancel " + dagNodeId+ " not found.");
    }
  }

  private void killByDagId(String dagId)
      throws IOException {
    if (this.dagManager.getDagToJobs().containsKey(dagId)) {
      List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel =
          this.dagManager.getDagToJobs().get(dagId);
      for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dagNodesToCancel) {
        cancelDagNode(dagNodeToCancel);
      }
    } else {
      log.warn("Dag to cancel " + dagId + " not found.");
    }
  }

  private void cancelDagNode(Dag.DagNode<JobExecutionPlan> dagNodeToCancel) throws IOException {
    Properties props = new Properties();
    try {
      if (dagNodeToCancel.getValue().getJobFuture().isPresent()) {
        Future future = dagNodeToCancel.getValue().getJobFuture().get();
        String serializedFuture = DagManagerUtils.getSpecProducer(dagNodeToCancel).serializeAddSpecResponse(future);
        props.put(ConfigurationKeys.SPEC_PRODUCER_SERIALIZED_FUTURE, serializedFuture);
        sendCancellationEvent(dagNodeToCancel.getValue());
      }
      if (dagNodeToCancel.getValue().getJobSpec().getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
        props.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY,
            dagNodeToCancel.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
      }
      DagManagerUtils.getSpecProducer(dagNodeToCancel).cancelJob(dagNodeToCancel.getValue().getJobSpec().getUri(), props);
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void sendCancellationEvent(JobExecutionPlan jobExecutionPlan) {
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
    this.eventSubmitter.get().getTimingEvent(TimingEvent.LauncherTimings.JOB_CANCEL).stop(jobMetadata);
    jobExecutionPlan.setExecutionStatus(CANCELLED);
  }
}

