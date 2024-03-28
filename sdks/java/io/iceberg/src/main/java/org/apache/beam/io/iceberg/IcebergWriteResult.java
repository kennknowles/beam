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
package org.apache.beam.io.iceberg;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;

public final class IcebergWriteResult<DestinationT, ElementT> implements POutput {

  private final Pipeline pipeline;
  PCollection<KV<ShardedKey<DestinationT>, ElementT>> successfulInserts;
  TupleTag<KV<ShardedKey<DestinationT>, ElementT>> successfulInsertsTag;

  PCollection<WriteBundlesToFiles.Result<DestinationT>> catalogUpdates;
  TupleTag<WriteBundlesToFiles.Result<DestinationT>> catalogUpdatesTag;

  PCollection<KV<String, Snapshot>> snapshots;

  TupleTag<KV<TableIdentifier, Snapshot>> snapshotsTag;

  public IcebergWriteResult(
      Pipeline pipeline,
      PCollection<KV<ShardedKey<DestinationT>, ElementT>> successfulInserts,
      PCollection<WriteBundlesToFiles.Result<DestinationT>> catalogUpdates,
      PCollection<KV<String, Snapshot>> snapshots,
      TupleTag<KV<ShardedKey<DestinationT>, ElementT>> successfulInsertsTag,
      TupleTag<WriteBundlesToFiles.Result<DestinationT>> catalogUpdatesTag,
      TupleTag<KV<TableIdentifier, Snapshot>> snapshotsTag) {
    this.pipeline = pipeline;
    this.successfulInserts = successfulInserts;
    this.catalogUpdates = catalogUpdates;
    this.snapshots = snapshots;

    this.successfulInsertsTag = successfulInsertsTag;
    this.catalogUpdatesTag = catalogUpdatesTag;
    this.snapshotsTag = snapshotsTag;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  public PCollection<KV<ShardedKey<DestinationT>, ElementT>> getSuccessfulInserts() {
    return successfulInserts;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> output = ImmutableMap.builder();
    if (successfulInsertsTag != null) {
      output.put(successfulInsertsTag, Preconditions.checkNotNull(successfulInserts));
    }
    if (catalogUpdatesTag != null) {
      output.put(catalogUpdatesTag, Preconditions.checkNotNull(catalogUpdates));
    }
    if (snapshotsTag != null) {
      output.put(snapshotsTag, Preconditions.checkNotNull(snapshots));
    }

    return output.build();
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
