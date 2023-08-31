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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Responsible to performing the actual work for a given {@link DagTask}.
 * It processes the {@link DagTask} by first initializing its state, performing actions
 * based on the type of {@link DagTask} and finally submitting an event to the executor.
 */
@Alpha
@Slf4j
public abstract class DagProc<S, T> {
  protected final DagProcessingEngine dagProcessingEngine;
  protected final Optional<EventSubmitter> eventSubmitter;
  protected final AtomicLong orchestrationDelay;
  private final MetricContext metricContext;

  public DagProc(DagProcessingEngine dagProcessingEngine) {
    this.dagProcessingEngine = dagProcessingEngine;
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build());
    this.orchestrationDelay = new AtomicLong(0);
    ContextAwareGauge<Long> orchestrationDelayMetric = metricContext.newContextAwareGauge(ServiceMetricNames.FLOW_ORCHESTRATION_DELAY,
        orchestrationDelay::get);
    metricContext.register(orchestrationDelayMetric);
  }

  public void process(DagManagementStateStore dagManagementStateStore) throws IOException {
    S state = initialize(dagManagementStateStore);   //retry
    T result = act(dagManagementStateStore, state);   //retry
    commit(dagManagementStateStore, result);   //retry
  }

  protected abstract S initialize(DagManagementStateStore dagManagementStateStore) throws IOException;

  protected abstract T act(DagManagementStateStore dagManagementStateStore, S state) throws IOException;

  private void commit(DagManagementStateStore dagManagementStateStore, T result) {
    // todo - commit the modified dags to the persistent store, maybe not required for InMem dagManagementStateStore
  }
}
