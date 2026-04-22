/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sparkconnect.handler;

import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.apache.beam.sparkconnect.ProtoUtils;
import org.apache.spark.connect.proto.ConfigRequest;
import org.apache.spark.connect.proto.ConfigResponse;
import org.apache.spark.connect.proto.KeyValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigHandler.class);
  private final Map<String, String> mutableConfig;

  public ConfigHandler(Map<String, String> mutableConfig) {
    this.mutableConfig = mutableConfig;
  }

  public void handle(ConfigRequest request, StreamObserver<ConfigResponse> responseObserver) {
    ConfigResponse.Builder responseBuilder = ConfigResponse.newBuilder();

    responseBuilder.setSessionId(request.getSessionId());
    responseBuilder.setServerSideSessionId(request.getSessionId());
    responseBuilder.addWarnings("This is fake");

    // TBD which config options we actually need or want to support; for now we pretend!
    switch (request.getOperation().getOpTypeCase()) {
      case SET:
        for (KeyValue pair : request.getOperation().getSet().getPairsList()) {
          mutableConfig.put(pair.getKey(), pair.getValue());
          if (pair.getKey().startsWith("spark.sql.execution.arrow.")
              && !pair.getKey().contains(".pyspark.")) {
            mutableConfig.put(pair.getKey().replace("arrow.", "arrow.pyspark."), pair.getValue());
          }
        }
        break;
      case GET:
        handleConfigGet(request.getOperation().getGet(), responseBuilder);
        break;
      case GET_WITH_DEFAULT:
        handleConfigGetWithDefault(request.getOperation().getGetWithDefault(), responseBuilder);
        break;
      case GET_OPTION:
        break;
      case GET_ALL:
        break;
      case UNSET:
        break;
      case IS_MODIFIABLE:
        for (String key : request.getOperation().getIsModifiable().getKeysList()) {
          responseBuilder.addPairs(KeyValue.newBuilder().setKey(key).setValue("false"));
        }
        break;
      case OPTYPE_NOT_SET:
        break;
    }

    LOG.debug("config response:\n{}", ProtoUtils.debugString(responseBuilder));
    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }

  private void handleConfigGet(ConfigRequest.Get request, ConfigResponse.Builder responseBuilder) {
    LOG.info("Handling Config GET for keys: {}", request.getKeysList());
    for (String key : request.getKeysList()) {
      KeyValue.Builder kvBuilder = KeyValue.newBuilder();
      kvBuilder.setKey(key);
      @Nullable String value = mutableConfig.get(key);
      if (value != null) {
        kvBuilder.setValue(value);
      }
      responseBuilder.addPairs(kvBuilder.build());
    }
  }

  private void handleConfigGetWithDefault(
      ConfigRequest.GetWithDefault request, ConfigResponse.Builder responseBuilder) {
    for (KeyValue keyWithDefault : request.getPairsList()) {
      KeyValue.Builder kvBuilder = KeyValue.newBuilder();
      kvBuilder.setKey(keyWithDefault.getKey());
      kvBuilder.setValue(
          mutableConfig.getOrDefault(keyWithDefault.getKey(), keyWithDefault.getValue()));
      responseBuilder.addPairs(kvBuilder.build());
    }
  }
}
