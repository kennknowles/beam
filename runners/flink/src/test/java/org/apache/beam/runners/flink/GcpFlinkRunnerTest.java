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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Test;

/** Test for {@link FlinkRunner}. */
public class GcpFlinkRunnerTest {

  @Test
  public void smokeTest() throws Exception {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply(Create.of(1, 2, 3));

    GcpFlinkRunnerOptions options = PipelineOptionsFactory.create().as(GcpFlinkRunnerOptions.class);
    options.setRunner(GcpFlinkRunner.class);
    options.setStagingLocation("gs://apache-beam-testing-kenn/gcpFlinkRunnerStaging");
    options.setLocation("us-east4");

    pipeline.run(options);
  }
}
