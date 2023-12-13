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

import java.net.URI;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagTaskVisitor;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;


/**
 * A {@link DagTask} responsible to handle kill tasks.
 */
@Alpha
public class KillDagTask extends DagTask {
  @Getter Dag.DagNode.DagNodeId dagNodeId;
  @Getter URI uri;

  public KillDagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseAttemptStatus leaseObtainedStatus) {
    super(dagAction, leaseObtainedStatus);
  }

  @Override
  public DagProc host(DagTaskVisitor<DagProc> visitor) {
    return visitor.meet(this);
  }
}
