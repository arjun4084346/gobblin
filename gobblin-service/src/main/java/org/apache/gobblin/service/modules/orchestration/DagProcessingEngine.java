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
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.AdvanceDagTask;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.orchestration.task.RetryDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Responsible for polling {@link DagTask}s from {@link DagTaskStream} and processing the {@link org.apache.gobblin.service.modules.flowgraph.Dag}
 * based on the type of {@link DagTask} which is determined by the {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}.
 * Each {@link DagTask} acquires a lease for the {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}.
 * The {@link DagProcFactory} then provides the appropriate {@link DagProc} associated with the {@link DagTask}.
 * The actual work or processing is done by the {@link DagProc#process(DagManagementStateStore, int, long)}
 */

@Alpha
@Slf4j
public class DagProcessingEngine {
  public static final String GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX = ServiceConfigKeys.GOBBLIN_SERVICE_PREFIX + "dagProcessingEngine.";
  public static final String GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_ENABLED_KEY = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX + "enabled";
  public static final String NUM_THREADS_KEY = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX + "numThreads";
  public static final String JOB_STATUS_POLLING_INTERVAL_KEY = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX + "pollingInterval";
  private static final Integer DEFAULT_NUM_THREADS = 3;
  private static final Integer DEFAULT_JOB_STATUS_POLLING_INTERVAL = 10;

  private final DagTaskStream dagTaskStream;
  Optional<EventSubmitter> eventSubmitter;

  public DagProcessingEngine(Config config, DagTaskStream dagTaskStream, DagProcFactory dagProcFactory, DagManagementStateStore dagManagementStateStore, MultiActiveLeaseArbiter multiActiveLeaseArbiter) {
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build());
    Integer numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    ScheduledExecutorService scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    Integer pollingInterval = ConfigUtils.getInt(config, JOB_STATUS_POLLING_INTERVAL_KEY, DEFAULT_JOB_STATUS_POLLING_INTERVAL);
    this.dagTaskStream = dagTaskStream;

    for (int i=0; i < numThreads; i++) {
      DagProcEngineThread dagProcEngineThread = new DagProcEngineThread(dagTaskStream, dagProcFactory);
      scheduledExecutorPool.scheduleAtFixedRate(dagProcEngineThread, 0, pollingInterval, TimeUnit.SECONDS);
    }
  }

  public void addNewDag(Dag<JobExecutionPlan> dag) {
    DagTask dagTask = new LaunchDagTask(dag);
    this.dagTaskStream.addDagTask(dagTask);
  }

  public void addDagNodeToRetry(Dag.DagNode<JobExecutionPlan> dagNode) {
    this.dagTaskStream.addDagTask(new RetryDagTask(dagNode));
  }

  public void addAdvanceDagTask(Dag.DagNode<JobExecutionPlan> dagNode) {
    this.dagTaskStream.addDagTask(new AdvanceDagTask(dagNode));
  }

  public void addDagTask(DagTask dagTask) {
    this.dagTaskStream.addDagTask(dagTask);
  }

  @AllArgsConstructor
  private static class DagProcEngineThread implements Runnable {

    private DagTaskStream dagTaskStream;
    private DagProcFactory dagProcFactory;

    @Override
    public void run() {
      for (DagTaskStream it = dagTaskStream; it.hasNext(); ) {
        DagTask dagTask = it.next();
        DagProc dagProc = dagProcFactory.getDagProcFor(dagTask);
//          dagProc.process(eventSubmitter.get(), maxRetryAttempts, delayRetryMillis);
        try {
          dagProc.process(dagTask);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        // todo mark lease success and releases it
        //dagTaskStream.complete(dagTask);
      }
    }
  }
}
