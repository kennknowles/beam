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
package org.apache.beam.sparkconnect.beamrel;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.spark.connect.proto.CommonInlineUserDefinedFunction;

public class BeamMapPartitions extends SingleRel implements BeamRelNode {

  public final CommonInlineUserDefinedFunction func;
  private final RelDataType outputRowType;

  public BeamMapPartitions(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      CommonInlineUserDefinedFunction func,
      RelDataType outputRowType) {
    super(cluster, traits, input);
    this.func = func;
    this.outputRowType = outputRowType;
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new PTransform<PCollectionList<Row>, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PCollectionList<Row> pinput) {
        return pinput.get(0);
      }
    };
  }

  @Override
  public RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    // We don't know how many rows the UDF will produce.
    // Let's assume it's the same as input for now.
    return mq.getNodeStats(input);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    // Assume some cost for running UDF
    return BeamCostModel.FACTORY.makeTinyCost();
  }

  @Override
  public BeamMapPartitions copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BeamMapPartitions(getCluster(), traitSet, sole(inputs), func, outputRowType);
  }
}
