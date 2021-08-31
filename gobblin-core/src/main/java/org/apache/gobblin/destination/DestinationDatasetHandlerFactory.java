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

package org.apache.gobblin.destination;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


public class DestinationDatasetHandlerFactory  {

  public static DestinationDatasetHandler newInstance(String handlerTypeName, State state, Boolean canCleanUp) {
    try {
      ClassAliasResolver<DestinationDatasetHandler> aliasResolver = new ClassAliasResolver<>(DestinationDatasetHandler.class);
      DestinationDatasetHandler handler = GobblinConstructorUtils.invokeLongestConstructor(
          aliasResolver.resolveClass(handlerTypeName), state, canCleanUp);
      return handler;
    } catch (ReflectiveOperationException rfe) {
      throw new RuntimeException("Could not construct DestinationDatasetHandler " + handlerTypeName, rfe);
    }
  }
}
