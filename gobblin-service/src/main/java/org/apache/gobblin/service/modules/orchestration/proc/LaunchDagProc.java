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

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcFactory;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An implementation of {@link DagProc} for launching {@link org.apache.gobblin.service.modules.orchestration.task.DagTask}.
 */
@Slf4j
@Alpha
public class LaunchDagProc extends DagProc<LaunchDagTask> {
  private final LaunchDagTask launchDagTask;
  private Optional<Dag<JobExecutionPlan>> dag;

  public LaunchDagProc(LaunchDagTask launchDagTask, DagProcFactory dagProcFactory) {
    super(dagProcFactory);
    this.launchDagTask = launchDagTask;
  }

  @Override
  protected void initialize() throws IOException {
    this.dag = dagManagementStateStore.getDag(this.launchDagTask.getDagId().toString());
    if (!this.dag.isPresent()) {
      log.warn("No dag with id " + this.launchDagTask.getDagId() + " found to launch");
      return;
    }
    initializeDag(this.dag.get());
  }

  @Override
  protected void act() throws IOException {
    if (!this.dag.isPresent()) {
      log.warn("No dag with id " + this.launchDagTask.getDagId() + " found to launch");
      return;
    }
    this.dagStateStore.writeCheckpoint(this.dag.get());
    for (Dag.DagNode<JobExecutionPlan> dagNode : this.dag.get().getStartNodes()) {
      this.dagProcFactory.dagProcessingEngine.addAdvanceDagAction(dagNode);
    }
    DagManagerUtils.submitEventsAndSetStatus(this.dag.get(), this.eventSubmitter);
  }
}
