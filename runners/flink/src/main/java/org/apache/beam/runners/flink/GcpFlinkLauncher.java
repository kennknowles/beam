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

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small "main" class that reads in a portable Beam pipeline and launches it using the Flink
 * runner.
 */
public class GcpFlinkLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(GcpFlinkLauncher.class);

  public interface GcpFlinkLauncherOptions extends FlinkPipelineOptions {

    @Description("Path to file containing Beam pipeline in protocol buffer format")
    @Validation.Required
    String getPipelineFile();

    void setPipelineFile(String file);
  }

  public static void main(String[] args) throws Exception {
    GcpFlinkLauncherOptions options =
        PipelineOptionsFactory.fromArgs(args).as(GcpFlinkLauncherOptions.class);
    Path pipelineFile = Paths.get(options.getPipelineFile()).toAbsolutePath();
    LOG.info("Loading Beam pipeline from " + pipelineFile);

    //    RunnerApi.Pipeline pipeline;
    //    try (InputStream inputStream = Files.newInputStream(pipelineFile)) {
    //      pipeline = RunnerApi.Pipeline.parseDelimitedFrom(inputStream);
    //    }

    // We don't have a TraverseTopologically that actually goes to sub-transforms, so we don't have
    // yet the mechanism to run this pipeline. Instead for today we just make a new pipeline and see
    // if the job runs

    options.setRunner(FlinkRunner.class);
    options.setStreaming(true);
    options.setFlinkMaster("[auto]");
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(1, 2, 3));
    p.run();

    // System.exit(1);
  }
}
