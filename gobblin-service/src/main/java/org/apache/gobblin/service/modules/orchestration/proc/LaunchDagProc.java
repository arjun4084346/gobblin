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

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An implementation of {@link DagProc} for launching {@link org.apache.gobblin.service.modules.orchestration.task.DagTask}.
 */
@Slf4j
@Alpha
public class LaunchDagProc extends DagProc<LaunchDagTask> {
  private final LaunchDagTask launchDagTask;
  private Dag<JobExecutionPlan> dag;

  public LaunchDagProc(LaunchDagTask launchDagTask, DagProcessingEngine dagProcessingEngine) {
    super(dagProcessingEngine);
    this.launchDagTask = launchDagTask;
  }

  @Override
  protected void initialize() throws IOException {
    this.dag = this.dagManager.getDag(this.launchDagTask.getDagId().toString());
    if (this.dag == null) {
      log.warn("No dag with id " + this.launchDagTask.getDagId() + " found to launch");
      return;
    }
    initializeDag(this.dag);
  }

  @Override
  protected void act() throws IOException {
    if (this.dag == null) {
      log.warn("No dag with id " + this.launchDagTask.getDagId() + " found to launch");
      return;
    }
    this.dagStateStore.writeCheckpoint(this.dag);
    for (Dag.DagNode<JobExecutionPlan> dagNode : this.dag.getStartNodes()) {
      this.dagProcessingEngine.addDagAction(dagNode);
    }
    DagManagerUtils.submitInitializationEventsAndSetStatus(this.dag, this.eventSubmitter);
  }
}
