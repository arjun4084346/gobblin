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
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.KillFlowEvent;
import org.apache.gobblin.service.monitoring.ResumeFlowEvent;
import org.apache.gobblin.service.monitoring.event.JobStatusEvent;


/**
 * Responsible for defining the behavior of {@link DagTask} handling scenarios for launch, resume, kill, job start
 * and flow completion deadlines
 *
 */
@Alpha
public interface DagManagement {

  DagStateStore createDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap);
  void deleteJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);
  void addJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);

  /**
   * defines what to do when a job (dag node) finishes
   * @param dagNode dag node that finished
   * @return next set of DagNodes to run
   * @throws IOException
   */
  Map<String, Set<Dag.DagNode<JobExecutionPlan>>> onJobFinish(Dag.DagNode<JobExecutionPlan> dagNode)
      throws IOException;

  /**
   * submit next dag nodes to run
   * @param dagId dag id for which next dag nodes to run
   * @return a set of dag nodes that were submitted to run by this method
   * @throws IOException
   */
  // todo : maybe return just a set
  Map<String, Set<Dag.DagNode<JobExecutionPlan>>> submitNext(String dagId) throws IOException;
  void removeDagActionFromStore(NewDagManager.DagId dagIdToResume, DagActionStore.FlowActionType flowActionType)
      throws IOException;

  void handleJobStatusEvent(JobStatusEvent jobStatusEvent);
  void handleKillFlowEvent(KillFlowEvent killFlowEvent);
  void handleLaunchFlowEvent(DagActionStore.DagAction launchAction);
  void handleResumeFlowEvent(ResumeFlowEvent resumeFlowEvent) throws IOException;

  Map<String, Dag<JobExecutionPlan>> getDags();
  Map<String, Dag<JobExecutionPlan>> getResumingDags();
  Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> getJobToDag();
  Map<String, Dag.DagNode<JobExecutionPlan>> getDagNodes();
  Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> getDagToJobs();
  Map<String, Long> getDagToSLA();
  Set<String> getFailedDagIds();
  DagStateStore getFailedDagStateStore();
  DagStateStore getDagStateStore();


  void setActive() throws IOException;
}
