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

package org.apache.gobblin.runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.quartz.SchedulerException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.DagActionReminderScheduler;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeEvent;
import org.apache.gobblin.service.monitoring.DagActionValue;
import org.apache.gobblin.service.monitoring.DagManagementDagActionStoreChangeMonitor;
import org.apache.gobblin.service.monitoring.GenericStoreChangeEvent;
import org.apache.gobblin.service.monitoring.OperationType;

import static org.mockito.Mockito.*;


/**
 * Tests the main functionality of {@link DagManagementDagActionStoreChangeMonitor} to process {@link DagActionStoreChangeEvent} type
 * events stored in a {@link org.apache.gobblin.kafka.client.KafkaConsumerRecord}. The
 * processMessage(DecodeableKafkaRecord message) function should be able to gracefully process a variety of message
 * types, even with undesired formats, without throwing exceptions.
 */
@Slf4j
public class DagManagementDagActionStoreChangeMonitorTest {
  public static final String TOPIC = DagActionStoreChangeEvent.class.getSimpleName();
  private final int PARTITION = 1;
  private final int OFFSET = 1;
  private final String FLOW_GROUP = "flowGroup";
  private final String FLOW_NAME = "flowName";
  private final long FLOW_EXECUTION_ID = 123L;
  private final String JOB_NAME = "jobName";
  private MockDagManagementDagActionStoreChangeMonitor mockDagManagementDagActionStoreChangeMonitor;
  private int txidCounter = 0;

  private static final DagActionReminderScheduler dagActionReminderScheduler = mock(DagActionReminderScheduler.class);

  /**
   * Note: The class methods are wrapped in a test specific method because the original methods are package protected
   * and cannot be accessed by this class.
   */
  static class MockDagManagementDagActionStoreChangeMonitor extends DagManagementDagActionStoreChangeMonitor {

    public MockDagManagementDagActionStoreChangeMonitor(Config config, int numThreads, boolean isMultiActiveSchedulerEnabled) {
      super(config, numThreads, mock(FlowCatalog.class), mock(Orchestrator.class), mock(DagManagementStateStore.class),
          isMultiActiveSchedulerEnabled, mock(DagManagement.class), dagActionReminderScheduler);
    }
    protected void processMessageForTest(DecodeableKafkaRecord<String, DagActionStoreChangeEvent> record) {
      super.processMessage(record);
    }
  }

  MockDagManagementDagActionStoreChangeMonitor createMockDagManagementDagActionStoreChangeMonitor() {
    Config config = ConfigFactory.empty().withValue(ConfigurationKeys.KAFKA_BROKERS, ConfigValueFactory.fromAnyRef("localhost:0000"))
        .withValue(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArrayDeserializer"))
        .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef("/tmp/fakeStateStore"))
        .withValue("zookeeper.connect", ConfigValueFactory.fromAnyRef("localhost:2121"));
    return new MockDagManagementDagActionStoreChangeMonitor(config, 5, true);
  }

  // Called at start of every test so the count of each method being called is reset to 0
  @BeforeMethod
  public void setupMockMonitor() {
     mockDagManagementDagActionStoreChangeMonitor = createMockDagManagementDagActionStoreChangeMonitor();
  }

  @BeforeClass
  public void setUp() throws Exception {
    doNothing().when(dagActionReminderScheduler).unscheduleReminderJob(any());

  }

  /**
   * Tests process message with a DELETE type message.
   */
  @Test
  public void testProcessMessageWithDelete() throws SchedulerException {
    Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.DELETE, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, JOB_NAME, DagActionValue.ENFORCE_JOB_START_DEADLINE);
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, JOB_NAME,
        DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE);
    mockDagManagementDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagManagementDagActionStoreChangeMonitor.getDagActionReminderScheduler(), times(1))
        .unscheduleReminderJob(eq(dagAction));
  }

  /**
   * Util to create a general DagActionStoreChange type event
   */
  private DagActionStoreChangeEvent createDagActionStoreChangeEvent(OperationType operationType,
      String flowGroup, String flowName, long flowExecutionId, String jobName, DagActionValue dagAction) {
    String key = DagActionStoreChangeMonitorTest.getKeyForFlow(flowGroup, flowName, flowExecutionId);
    GenericStoreChangeEvent genericStoreChangeEvent =
        new GenericStoreChangeEvent(key, String.valueOf(txidCounter), System.currentTimeMillis(), operationType);
    txidCounter++;
    return new DagActionStoreChangeEvent(genericStoreChangeEvent, flowGroup, flowName, String.valueOf(flowExecutionId),
        jobName, dagAction);
  }

  /**
   * Util to create wrapper around DagActionStoreChangeEvent
   */
  private Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> wrapDagActionStoreChangeEvent(
      OperationType operationType, String flowGroup, String flowName, long flowExecutionId, String jobName, DagActionValue dagAction) {
    DagActionStoreChangeEvent eventToProcess = null;
    try {
      eventToProcess =
          createDagActionStoreChangeEvent(operationType, flowGroup, flowName, flowExecutionId, jobName, dagAction);
    } catch (Exception e) {
      log.error("Exception while creating event ", e);
    }
    // TODO: handle partition and offset values better
    ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET,
        DagActionStoreChangeMonitorTest.getKeyForFlow(flowGroup, flowName, flowExecutionId), eventToProcess);
    return new Kafka09ConsumerClient.Kafka09ConsumerRecord<>(consumerRecord);
  }
}