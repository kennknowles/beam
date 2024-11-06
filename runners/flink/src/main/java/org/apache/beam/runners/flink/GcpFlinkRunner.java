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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarOutputStream;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.FileStagingOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.resources.PipelineResources;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runner that launches Flink pipelines on Google Cloud Platform's "BigQuery Engine for Apache
 * Flink"
 */
public class GcpFlinkRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(GcpFlinkRunner.class);

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties which configure the runner.
   * @return The newly created runner.
   */
  public static GcpFlinkRunner fromOptions(PipelineOptions options) {
    return new GcpFlinkRunner(
        PipelineOptionsValidator.validate(GcpFlinkRunnerOptions.class, options));
  }

  private final GcpFlinkRunnerOptions options;

  private GcpFlinkRunner(GcpFlinkRunnerOptions options) {
    this.options = options;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    try {
      return runOrThrow(pipeline);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private PipelineResult runOrThrow(Pipeline pipeline) throws Exception {

    // Save the proto to a file to be loaded up by the launcher.
    Path protoFile = Files.createTempFile("flink-beam-pipeline-", ".proto");
    RunnerApi.Pipeline portablePipelineProto = PipelineTranslation.toProto(pipeline);
    //    LOG.info(
    //        "Pipeline proto:\n{}", TextFormat.printer().printToString(portablePipelineProto));

    try (OutputStream outputStream = Files.newOutputStream(protoFile)) {
      portablePipelineProto.writeDelimitedTo(outputStream);
    }

    // Since the classpath has everything we need and we will specify a main class, we
    // just create a temporary empty jar to pass to gcloud
    Path mainJarFile = Files.createTempFile("flink-dummy-jar-", ".jar").toAbsolutePath();
    new JarOutputStream(Files.newOutputStream(mainJarFile)).close();

    // This prunes empty files and also defaults filesToStage to "everything on the classpath" if
    // empty
    PipelineResources.prepareFilesForStaging(options.as(FileStagingOptions.class));

    List<String> command =
        ImmutableList.<String>builder()
            .add(
                "gcloud",
                "alpha",
                "managed-flink",
                "jobs",
                "create",
                mainJarFile.toString(),
                "--staging-location=" + options.getStagingLocation(),
                "--location=" + options.getLocation(),
                "--jars=" + Joiner.on(",").join(options.getFilesToStage()),
                "--class=" + GcpFlinkLauncher.class.getCanonicalName(),
                "--min-parallelism=3",
                "--max-parallelism=100",
                "--enable-output",
                "--verbosity=debug",
                "--",
                "--pipelineFile=" + protoFile)
            .build();

    LOG.info("Executing x x gcloud command: " + Joiner.on(" ").join(command));

    // Executes gcloud and pipes the stdout / stderr for now, but we will switch to
    // GcpPipelineResult shortly

    ProcessBuilder processBuilder = new ProcessBuilder().command(command).redirectErrorStream(true);
    processBuilder.environment().putAll(System.getenv());
    Process gcloudProcess = processBuilder.start();

    try (InputStream gcloudOutput = gcloudProcess.getInputStream()) {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(gcloudOutput, StandardCharsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        LOG.info("[gcloud] {}", line);
      }
    }

    boolean hasTerminated = gcloudProcess.waitFor(10, TimeUnit.MINUTES);

    if (!hasTerminated) {
      throw new RuntimeException("gcloud command timed out");
    }

    if (gcloudProcess.exitValue() != 0) {
      throw new RuntimeException("gcloud exited  with error code " + gcloudProcess.exitValue());
    }

    System.out.flush();
    System.err.flush();
    return new GcpFlinkPipelineResult();
  }
}
