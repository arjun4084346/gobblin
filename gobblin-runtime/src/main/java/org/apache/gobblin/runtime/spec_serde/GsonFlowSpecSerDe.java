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

package org.apache.gobblin.runtime.spec_serde;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecSerDeException;


/**
 * {@link SpecSerDe} that serializes as Json using {@link Gson}. Note that currently only {@link FlowSpec}s are supported.
 */
public class GsonFlowSpecSerDe implements SpecSerDe {
  private GsonSerDe<FlowSpec> gsonSerDe;

  public GsonFlowSpecSerDe() {
    this.gsonSerDe = new GsonSerDe<>(new TypeToken<FlowSpec>(){}.getType(), new FlowSpecSerializer(), new FlowSpecDeserializer());
  }

  @Override
  public byte[] serialize(Spec spec) throws SpecSerDeException {
    if (!(spec instanceof FlowSpec)) {
      throw new SpecSerDeException("Failed to serialize spec " + spec.getUri() + ", only FlowSpec is supported");
    }

    try {
      return this.gsonSerDe.serialize((FlowSpec) spec).getBytes(Charsets.UTF_8);
    } catch (JsonParseException e) {
      throw new SpecSerDeException(spec, e);
    }
  }

  @Override
  public Spec deserialize(byte[] spec) throws SpecSerDeException {
    try {
      return this.gsonSerDe.deserialize(new String(spec, Charsets.UTF_8));
    } catch (JsonParseException e) {
      throw new SpecSerDeException(e);
    }
  }
}
