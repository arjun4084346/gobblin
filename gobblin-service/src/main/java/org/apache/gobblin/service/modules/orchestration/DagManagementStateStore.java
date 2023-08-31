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
import java.util.LinkedList;

import com.google.common.base.Optional;

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An interface to provide abstractions for managing {@link Dag} and {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} states
 * and allows add/delete and other functions
 */
public interface DagManagementStateStore {

//  public void addDag(Dag<JobExecutionPlan> dag);

  public void addJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);

  public void deleteJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);

  public boolean hasRunningJobs(String dagId);

  public void removeDagActionFromStore(DagManager.DagId dagId, DagActionStore.FlowActionType flowActionType) throws IOException;

//  public void addDagStartDeadline(String dagId, Long flowStartSla);
//  public Long getDagStartDeadline(String dagId);
  public void addDagDeadline(String dagId, Long flowSla);
  public Long getDagDeadline(String dagId);

  public Optional<Dag<JobExecutionPlan>> getDag(String dagId);

  public LinkedList<Dag.DagNode<JobExecutionPlan>> getJobs(String dagId) throws IOException;

  public boolean addFailedDag(String dagId);

  public boolean existsFailedDag(String dagId);

  public boolean addCleanUpDag(String dagId);

  public boolean checkCleanUpDag(String dagId);


  //TODO: Add get methods for dags and jobs

  }
