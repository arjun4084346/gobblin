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
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;


/**
 * Holds a stream of {@link DagTask}s that {@link DagProcessingEngine} would pull from for processing.
 * It provides an implementation for {@link DagManagement} that defines the rules for a flow and job.
 * Implements {@link Iterable} to provide {@link DagTask}s as soon as it's available to {@link DagProcessingEngine}
 */

@Alpha
@Slf4j
@AllArgsConstructor

// change to iterable
public class DagTaskStream implements Iterator<DagTask>{
  private BlockingQueue<DagTask> dagActionQueue;
  public DagTaskStream() {
    this.dagActionQueue = new LinkedBlockingDeque<>();
  }

  @Override
  public boolean hasNext() {
    return !this.dagActionQueue.isEmpty();
  }

  @Override
  public DagTask next() {
    DagTask dagTask = this.dagActionQueue.poll();
    assert dagTask != null;
    try {
      // todo reconsider the use of MultiActiveLeaseArbiter
//      MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus =
//          flowTriggerHandler.getLeaseOnDagAction(jobProps, dagAction, System.currentTimeMillis());
//      if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
        // can it return null? is this iterator allowed to return null?
        return dagTask;
      //}
    } catch (Exception e) {
      //TODO: need to handle exceptions gracefully
      log.error("Error creating DagTask", e);
      return null;
    }
  }

  protected void complete(DagTask dagTask) throws IOException {
    //dagTask.conclude(this.flowTriggerHandler.getMultiActiveLeaseArbiter());
  }

  public void addDagTask(DagTask dagTask) {
    this.dagActionQueue.add(dagTask);
  }
}
