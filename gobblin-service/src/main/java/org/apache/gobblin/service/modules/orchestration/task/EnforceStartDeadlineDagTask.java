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

package org.apache.gobblin.service.modules.orchestration.task;

import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagTaskVisitor;
import org.apache.gobblin.service.modules.orchestration.LeaseAttemptStatus;


/**
 * A {@link DagTask} responsible for killing jobs if they have not started in
 * {@link org.apache.gobblin.service.modules.orchestration.DagManager#JOB_START_SLA_TIME} or does not finish in
 * {@link org.apache.gobblin.configuration.ConfigurationKeys#GOBBLIN_FLOW_SLA_TIME} time.
 */

public class EnforceStartDeadlineDagTask extends DagTask {
  public EnforceStartDeadlineDagTask(DagActionStore.DagAction dagAction, LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus,
      DagActionStore dagActionStore) {
    super(dagAction, leaseObtainedStatus, dagActionStore);
  }

  public <T> T host(DagTaskVisitor<T> visitor) {
    return visitor.meet(this);
  }
}
