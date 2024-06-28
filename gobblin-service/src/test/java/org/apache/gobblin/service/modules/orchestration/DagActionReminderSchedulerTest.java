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

import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.OperableTrigger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;

import org.apache.gobblin.configuration.ConfigurationKeys;


public class DagActionReminderSchedulerTest {
  String flowGroup = "fg";
  String flowName = "fn";
  long flowExecutionId = 123L;
  String jobName = "jn";
  String expectedKey =  Joiner.on(".").join(flowGroup, flowName, flowExecutionId, jobName,
      DagActionStore.DagActionType.LAUNCH.name());
  DagActionStore.DagAction launchDagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName,
      DagActionStore.DagActionType.LAUNCH);

  @Test
  public void testCreateDagActionReminderKey() {
    Assert.assertEquals(expectedKey, DagActionReminderScheduler.createDagActionReminderKey(launchDagAction));
  }

  @Test
  public void testCreateReminderJobTrigger() {
    long reminderDuration = 666L;
    Supplier<Long> getCurrentTimeMillis = () -> 12345600000L;
    Trigger reminderTrigger = DagActionReminderScheduler
        .createReminderJobTrigger(launchDagAction, reminderDuration, getCurrentTimeMillis, false);
    Assert.assertEquals(reminderTrigger.getKey().toString(), DagActionReminderScheduler.RetryReminderKeyGroup + "." + expectedKey);
    List<Date> fireTimes = TriggerUtils.computeFireTimes((OperableTrigger) reminderTrigger, null, 1);
    Assert.assertEquals(fireTimes.get(0), new Date(reminderDuration + getCurrentTimeMillis.get()));
  }

  @Test
  public void testCreateReminderJobDetail() {
    long expectedEventTimeMillis = 55L;
    JobDetail jobDetail = DagActionReminderScheduler.createReminderJobDetail(new DagActionStore.DagActionLeaseObject(launchDagAction, false, expectedEventTimeMillis), false);
    Assert.assertEquals(jobDetail.getKey().toString(), DagActionReminderScheduler.RetryReminderKeyGroup + "." + expectedKey);
    JobDataMap dataMap = jobDetail.getJobDataMap();
    Assert.assertEquals(dataMap.get(ConfigurationKeys.FLOW_GROUP_KEY), flowGroup);
    Assert.assertEquals(dataMap.get(ConfigurationKeys.FLOW_NAME_KEY), flowName);
    Assert.assertEquals(dataMap.get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), flowExecutionId);
    Assert.assertEquals(dataMap.get(ConfigurationKeys.JOB_NAME_KEY), jobName);
    Assert.assertEquals(dataMap.get(DagActionReminderScheduler.ReminderJob.FLOW_ACTION_TYPE_KEY),
        DagActionStore.DagActionType.LAUNCH);
    Assert.assertEquals(dataMap.get(DagActionReminderScheduler.ReminderJob.FLOW_ACTION_EVENT_TIME_KEY), expectedEventTimeMillis);
  }

  @Test
  public void testCreateReminderJobDetail2() throws SchedulerException, InterruptedException {

    JobDetail job = JobBuilder.newJob(MyJob.class)
        .withIdentity("myJob", "group1")
        .build();

    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity("myTrigger", "group1")
        .startAt(new Date(System.currentTimeMillis() + 2000L))
        .build(); // One-time trigger

    Scheduler scheduler = new StdSchedulerFactory().getScheduler();
    scheduler.start();
    scheduler.unscheduleJob(new TriggerKey("myTrigger", "group1"));

    scheduler.scheduleJob(job, trigger);

    // Before trigger fires
    System.out.println("Job exists before trigger fires: " + scheduler.checkExists(job.getKey()));
    System.out.println("Trigger exists before trigger fires: " + scheduler.checkExists(trigger.getKey()));

// Wait for the trigger to fire (in a real scenario, you would let the scheduler run naturally)
    Thread.sleep(3000);

// After trigger fires
    System.out.println("Job exists after trigger fires: " + scheduler.checkExists(job.getKey()));
    System.out.println("Trigger exists after trigger fires: " + scheduler.checkExists(trigger.getKey()));


  }
}
