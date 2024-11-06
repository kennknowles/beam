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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface GcpFlinkRunnerOptions extends FlinkPipelineOptions, GcpOptions {
  /**
   * The Compute Engine region (https://cloud.google.com/compute/docs/regions-zones/regions-zones)
   * in which worker processing should occur, e.g. "us-west1". Mutually exclusive with {@link
   * #getWorkerZone()}. If neither workerRegion nor workerZone is specified, default to same value
   * as region.
   */
  @Description(
      "The BigQuery Engine for Apache Flink location"
          + "(https://cloud.google.com/bigquery-engine-for-apache-flink/docs/locations) in which "
          + "processing should occur, e.g. \"us-west1\".")
  String getLocation();

  void setLocation(String workerRegion);

  @Description(
      "GCS path for staging local files, e.g. \"gs://bucket/object\". "
          + "Must be a valid Cloud Storage URL, beginning with the prefix \"gs://\". "
          + "If stagingLocation is unset, defaults to gcpTempLocation with \"/staging\" suffix.")
  @Default.InstanceFactory(StagingLocationFactory.class)
  String getStagingLocation();

  void setStagingLocation(String value);

  /** Returns a default staging location under {@link GcpOptions#getGcpTempLocation}. */
  class StagingLocationFactory implements DefaultValueFactory<String> {
    private static final Logger LOG = LoggerFactory.getLogger(StagingLocationFactory.class);

    @Override
    public String create(PipelineOptions options) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      LOG.info("No stagingLocation provided, falling back to gcpTempLocation");
      String gcpTempLocation;
      try {
        gcpTempLocation = checkStateNotNull(gcsOptions.getGcpTempLocation());
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Error constructing default value for stagingLocation: failed to retrieve"
                + " gcpTempLocation. Either stagingLocation must be set explicitly or a valid value"
                + " must be provided for gcpTempLocation.",
            e);
      }
      try {
        gcsOptions.getPathValidator().validateOutputFilePrefixSupported(gcpTempLocation);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Error constructing default value for stagingLocation: gcpTempLocation is not"
                    + " a valid GCS path, %s. ",
                gcpTempLocation),
            e);
      }
      return FileSystems.matchNewResource(gcpTempLocation, true /* isDirectory */)
          .resolve("staging", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
          .toString();
    }
  }
}
