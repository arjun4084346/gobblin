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

package org.apache.gobblin.cluster;

import java.util.concurrent.TimeUnit;

import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.JobExecutionLauncher;

/**
 * Metrics that relates to jobs launched by {@link GobblinHelixJobLauncher}.
 */
class GobblinHelixJobLauncherMetrics extends StandardMetricsBridge.StandardMetrics {
    private final String metricsName;

    public static final String METRIC_PREFIX = GobblinHelixJobLauncherMetrics.class.getSimpleName() + ".";
    public static final String NUM_JOBS_LAUNCHED = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.NUM_JOBS_LAUNCHED;
    public static final String NUM_JOBS_COMPLETED = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.NUM_JOBS_COMPLETED;
    public static final String NUM_JOBS_COMMITTED = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.NUM_JOBS_COMMITTED;
    public static final String NUM_JOBS_FAILED = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.NUM_JOBS_FAILED;
    public static final String NUM_JOBS_CANCELLED = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.NUM_JOBS_CANCELLED;
    public static final String TIMER_FOR_COMPLETED_JOBS = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.TIMER_FOR_COMPLETED_JOBS;
    public static final String TIMER_FOR_FAILED_JOBS = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.TIMER_FOR_FAILED_JOBS;
    public static final String TIMER_FOR_COMMITTED_JOBS = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.TIMER_FOR_COMMITTED_JOBS;
    public static final String NUM_JOBS_RUNNING = METRIC_PREFIX + JobExecutionLauncher.StandardMetrics.NUM_JOBS_RUNNING;

    final ContextAwareMeter numJobsLaunched;
    final ContextAwareMeter numJobsCompleted;
    final ContextAwareMeter numJobsCommitted;
    final ContextAwareMeter numJobsFailed;
    final ContextAwareMeter numJobsCancelled;
    final ContextAwareTimer timeForCompletedJobs;
    final ContextAwareTimer timeForFailedJobs;
    final ContextAwareTimer timeForCommittedJobs;
    final ContextAwareGauge<Integer> numJobsRunning;

    public GobblinHelixJobLauncherMetrics(String metricsName, final MetricContext metricContext, int windowSizeInMin) {
      this.metricsName = metricsName;

      this.numJobsLaunched = metricContext.contextAwareMeter(NUM_JOBS_LAUNCHED);
      this.contextAwareMetrics.add(this.numJobsLaunched);

      this.numJobsCompleted = metricContext.contextAwareMeter(NUM_JOBS_COMPLETED);
      this.contextAwareMetrics.add(this.numJobsCompleted);

      this.numJobsCommitted = metricContext.contextAwareMeter(NUM_JOBS_COMMITTED);
      this.contextAwareMetrics.add(this.numJobsCommitted);

      this.numJobsFailed = metricContext.contextAwareMeter(NUM_JOBS_FAILED);
      this.contextAwareMetrics.add(this.numJobsFailed);

      this.numJobsCancelled = metricContext.contextAwareMeter(NUM_JOBS_CANCELLED);
      this.contextAwareMetrics.add(this.numJobsCancelled);

      this.timeForCompletedJobs = metricContext.contextAwareTimer(TIMER_FOR_COMPLETED_JOBS, windowSizeInMin, TimeUnit.MINUTES);
      this.contextAwareMetrics.add(this.timeForCompletedJobs);

      this.timeForFailedJobs = metricContext.contextAwareTimer(TIMER_FOR_FAILED_JOBS, windowSizeInMin, TimeUnit.MINUTES);
      this.contextAwareMetrics.add(this.timeForFailedJobs);

      this.timeForCommittedJobs = metricContext.contextAwareTimer(TIMER_FOR_COMMITTED_JOBS, windowSizeInMin, TimeUnit.MINUTES);
      this.contextAwareMetrics.add(this.timeForCommittedJobs);

      this.numJobsRunning = metricContext.newContextAwareGauge(NUM_JOBS_RUNNING,
          () -> (int) (this.numJobsLaunched.getCount() - this.numJobsCompleted.getCount()));
      this.contextAwareMetrics.add(this.numJobsRunning);
    }

    @Override
    public String getName() {
      return this.metricsName;
    }
}
