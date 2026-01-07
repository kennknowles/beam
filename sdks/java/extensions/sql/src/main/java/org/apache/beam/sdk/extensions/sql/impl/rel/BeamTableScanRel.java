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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.rel.hint.RelHint;

/** HACK: only works with SubstraitTable */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamTableScanRel extends TableScan implements BeamRelNode {
  public BeamTableScanRel(
      RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
    super(cluster, traitSet, hints, table);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private static class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollectionList<Row> input) {
      checkArgument(
          input.expand().isEmpty(),
          "Wrong number of inputs for %s, expected 0 input but received: %s",
          BeamTableScanRel.class.getSimpleName(),
          input);
      throw new UnsupportedOperationException("Does not support table scan yet");
    }
  }

  @Override
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    return NodeStats.UNKNOWN;
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    NodeStats estimates = BeamSqlRelUtils.getNodeStats(this, mq);
    return BeamCostModel.FACTORY.makeCost(estimates.getRowCount(), estimates.getRate());
  }
}
