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
import java.util.ArrayList;
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

  // should dag task be a part of dag proc?
  private final KillDagTask killDagTask;
  private final List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel = new ArrayList<>();
  private Dag<JobExecutionPlan> dagToCancel;

  public KillDagProc(KillDagTask killDagTask, DagProcFactory dagProcFactory) {
    super(dagProcFactory);
    this.killDagTask = killDagTask;
  }

  protected void initialize() throws IOException {
    this.dagNodesToCancel.addAll(this.dagManager.getJobs(this.killDagTask.getDagId().toString()));
    this.dagToCancel = this.dagManager.getDag(this.killDagTask.getDagId().toString());
  }

  @Override
  public void act() throws IOException {
    if (this.dagNodesToCancel == null || this.dagNodesToCancel.isEmpty()) {
      log.warn("No dag with id " + this.killDagTask.getDagId() + " found to kill");
      return;
    }
    for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : this.dagNodesToCancel) {
      cancelDagNode(dagNodeToCancel);
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
      this.dagToCancel.setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
      this.dagToCancel.setMessage("Flow killed by request");
      this.dagManager.removeDagActionFromStore(this.killDagTask.getDagAction());
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

