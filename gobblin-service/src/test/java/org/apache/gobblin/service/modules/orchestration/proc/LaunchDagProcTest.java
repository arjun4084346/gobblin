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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.MockedSpecExecutor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.AzkabanProjectConfig;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerTest;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.util.ConfigUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


public class LaunchDagProcTest {
  private ITestMetastoreDatabase testMetastoreDatabase;
  private MostlyMySqlDagManagementStateStore dagManagementStateStore;

  @BeforeClass
  public void setUp() throws Exception {
    this.testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    this.dagManagementStateStore = spy(MostlyMySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));
    doReturn(FlowSpec.builder().build()).when(this.dagManagementStateStore).getFlowSpec(any());
    doNothing().when(this.dagManagementStateStore).tryAcquireQuota(any());
    doNothing().when(this.dagManagementStateStore).addDagNodeState(any(), any());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testMetastoreDatabase.close();
  }

  @Test
  public void launchDag()
      throws IOException, InterruptedException, URISyntaxException {
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", System.currentTimeMillis(), DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        5, "user5", ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("fg")));
    FlowCompilationValidationHelper flowCompilationValidationHelper = mock(FlowCompilationValidationHelper.class);
    doReturn(com.google.common.base.Optional.of(dag)).when(flowCompilationValidationHelper).createExecutionPlanIfValid(any());
    LaunchDagProc launchDagProc = new LaunchDagProc(
        new LaunchDagTask(new DagActionStore.DagAction("fg", "fn", "12345",
            "jn", DagActionStore.DagActionType.LAUNCH), null, this.dagManagementStateStore),
        flowCompilationValidationHelper);

    launchDagProc.process(this.dagManagementStateStore);
    int expectedNumOfSavingDagNodeStates = 1; // = number of start nodes
    Assert.assertEquals(expectedNumOfSavingDagNodeStates,
        Mockito.mockingDetails(this.dagManagementStateStore).getInvocations().stream()
            .filter(a -> a.getMethod().getName().equals("addDagNodeState")).count());

    List<Invocation> invocations = Mockito.mockingDetails(this.dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("addFlowDagAction")).collect(Collectors.toList());
    Assert.assertEquals(invocations.size(), 1);
    Assert.assertEquals(invocations.get(0).getArguments()[3], DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE);
  }

  // This creates a dag like this
  //  D1  D2 D3
  //    \ | /
  //     DN4
  //    / | \
  //  D5 D6  D7

  // Not used now, but can be used in GOBBLIN-2017
  public static Dag<JobExecutionPlan> buildDagWithMultipleNodesAtDifferentLevels(String id, Long flowExecutionId, String flowFailureOption,
      String proxyUser, Config additionalConfig)
      throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < 7; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).
          addPrimitive(ConfigurationKeys.FLOW_FAILURE_OPTION, flowFailureOption).
          addPrimitive(AzkabanProjectConfig.USER_TO_PROXY, proxyUser).build();
      jobConfig = additionalConfig.withFallback(jobConfig);
      if (i == 3) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0,job1,job2"));
      } else if ((i == 4) || (i == 5) || (i == 6)) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job3"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = MockedSpecExecutor.createDummySpecExecutor(new URI(
          ConfigUtils.getString(additionalConfig, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,"job" + i)));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }
}
