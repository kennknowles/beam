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
package org.apache.beam.sparkconnect;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A lightweight, thread-safe registry for managing fitted or loaded Spark ML models cached on the
 * server, allowing subsequent relational operations to retrieve them by their unique ObjectRef
 * identifiers.
 */
public class SparkMLObjectRegistry {

  private static final SparkMLObjectRegistry GLOBAL_INSTANCE = new SparkMLObjectRegistry();

  private final Map<String, ObjectRefState> registry = new ConcurrentHashMap<>();

  private SparkMLObjectRegistry() {}

  public static SparkMLObjectRegistry getGlobalRegistry() {
    return GLOBAL_INSTANCE;
  }

  /** Registers a loaded model and returns its generated ObjectRef ID. */
  public String register(String operatorName, String uid, String path) {
    String id = "model_" + UUID.randomUUID().toString();
    registry.put(id, new ObjectRefState(operatorName, uid, path));
    return id;
  }

  /** Retrieves the registered model state for a given ObjectRef ID. */
  public @Nullable ObjectRefState get(String id) {
    return registry.get(id);
  }

  /** Removes/Evicts a registered model from the cache. */
  public void evict(String id) {
    registry.remove(id);
  }

  /** Clears all registered models. */
  public void clear() {
    registry.clear();
  }

  /** State class containing deserialization info for a cached model. */
  public static class ObjectRefState {
    private final String operatorName;
    private final String uid;
    private final String path;

    public ObjectRefState(String operatorName, String uid, String path) {
      this.operatorName = operatorName;
      this.uid = uid;
      this.path = path;
    }

    public String getOperatorName() {
      return operatorName;
    }

    public String getUid() {
      return uid;
    }

    public String getPath() {
      return path;
    }
  }
}
