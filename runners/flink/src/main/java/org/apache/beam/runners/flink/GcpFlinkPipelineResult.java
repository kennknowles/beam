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
package org.apache.beam.runners.flink;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

/**
 * Result of executing a Pipeline using the {@link GcpFlinkRunner}. It gets its status by polling
 * the APIs.
 */
public class GcpFlinkPipelineResult implements PipelineResult {

  @Override
  public State getState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public State waitUntilFinish(@UnknownKeyFor @NonNull @Initialized Duration duration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public State waitUntilFinish() {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException();
  }
}
