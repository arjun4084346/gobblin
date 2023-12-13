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
import java.util.Map;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcFactory;
import org.apache.gobblin.service.modules.orchestration.NewDagManager;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;
import static org.apache.gobblin.service.ExecutionStatus.FAILED;
import static org.apache.gobblin.service.ExecutionStatus.PENDING_RESUME;


/**
 * An implementation of {@link DagProc} for resuming {@link org.apache.gobblin.service.modules.orchestration.task.DagTask}.
 */

@Alpha
@Slf4j
public final class ResumeDagProc extends DagProc {

  private ResumeDagTask resumeDagTask;
  Dag<JobExecutionPlan> dagToResume;
  NewDagManager.DagId dagIdToResume;


  public ResumeDagProc(ResumeDagTask resumeDagTask, DagProcFactory dagProcFactory) {
    super(dagProcFactory);
    this.resumeDagTask = resumeDagTask;
  }

  @Override
  protected void initialize() throws IOException {
    this.dagIdToResume = this.resumeDagTask.getDagId();
    this.dagToResume = this.dagManager.getFailedDagStateStore().getDag(this.resumeDagTask.getDagId().toString());
  }

  @Override
  protected void act() throws IOException {
    if (this.dagToResume == null || !this.dagManager.getFailedDagIds().contains(dagToResume.toString())) {
      log.warn("No dag found with dagId " + dagIdToResume + ", so cannot resume flow");
      this.dagManager.removeDagActionFromStore(dagIdToResume, DagActionStore.FlowActionType.RESUME);
      return;
    }

    long flowResumeTime = System.currentTimeMillis();

    for (Dag.DagNode<JobExecutionPlan> node : dagToResume.getNodes()) {
      ExecutionStatus executionStatus = node.getValue().getExecutionStatus();
      if (executionStatus.equals(FAILED) || executionStatus.equals(CANCELLED)) {
        node.getValue().setExecutionStatus(PENDING_RESUME);
        // reset currentAttempts because we do not want to count previous execution's attempts in deciding whether to retry a job
        node.getValue().setCurrentAttempts(0);
        DagManagerUtils.incrementJobGeneration(node);
        Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), node.getValue());
        this.dagManager.getEventSubmitter().get().getTimingEvent(TimingEvent.LauncherTimings.JOB_PENDING_RESUME).stop(jobMetadata);
      }

      // Set flowStartTime so that flow SLA will be based on current time instead of original flow
      node.getValue().setFlowStartTime(flowResumeTime);
    }

    JobStatus flowStatus = DagManagerUtils.pollFlowStatus(dagToResume, this.dagManager.getJobStatusRetriever(), this.dagManager.getJobStatusPolledTimer());
    if (flowStatus == null || !flowStatus.getEventName().equals(PENDING_RESUME.name())) {
      return;
    }

    boolean dagReady = true;
    for (Dag.DagNode<JobExecutionPlan> node : dagToResume.getNodes()) {
      JobStatus jobStatus = DagManagerUtils.pollJobStatus(node, this.dagManager.getJobStatusRetriever(), this.dagManager.getJobStatusPolledTimer());
      if (jobStatus == null || jobStatus.getEventName().equals(FAILED.name()) || jobStatus.getEventName().equals(CANCELLED.name())) {
        dagReady = false;
        break;
      }
    }

    if (dagReady) {
      this.dagStateStore.writeCheckpoint(dagToResume);
      this.dagManager.getFailedDagStateStore().cleanUp(dagToResume);
      this.dagManager.removeDagActionFromStore(dagIdToResume, DagActionStore.FlowActionType.RESUME);
      this.dagManager.getFailedDagIds().remove(dagIdToResume.toString());
      this.dagManager.getResumingDags().remove(dagIdToResume.toString());
      initializeDag(dagToResume);
    }
  }
}
