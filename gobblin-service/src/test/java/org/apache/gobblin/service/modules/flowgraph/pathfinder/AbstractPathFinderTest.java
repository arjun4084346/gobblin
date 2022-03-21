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

package org.apache.gobblin.service.modules.flowgraph.pathfinder;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.ServiceConfigKeys;


public class AbstractPathFinderTest {

  @Test
  public void convertDataNodesTest() {
    Config flowConfig = ConfigFactory.empty()
        .withValue(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, ConfigValueFactory.fromAnyRef("node1-alpha,node2"));
    Config sysConfig = ConfigFactory.empty()
        .withValue(AbstractPathFinder.DATA_NODE_ID_TO_ALIAS_MAP, ConfigValueFactory.fromAnyRef("node1-alpha:node1, node1-beta:node, node3-alpha:node3, node3-beta:node3"));


    List<String> dataNodes = AbstractPathFinder.getDataNodes(flowConfig, ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, sysConfig);
    Assert.assertEquals(dataNodes.size(), 2);
    Assert.assertTrue(dataNodes.contains("node1"));
    Assert.assertTrue(dataNodes.contains("node2"));
  }
}
