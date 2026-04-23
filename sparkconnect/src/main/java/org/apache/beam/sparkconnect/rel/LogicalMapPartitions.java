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
package org.apache.beam.sparkconnect.rel;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
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

/** A custom {@code RelNode} to represent the Spark {@code mapPartitions} operation. */
public class LogicalMapPartitions extends SingleRel implements BeamRelNode {

  public final CommonInlineUserDefinedFunction func;
  private final RelDataType specifiedRowType;

  public LogicalMapPartitions(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      CommonInlineUserDefinedFunction func,
      RelDataType specifiedRowType) {
    super(cluster, traits, input);
    this.func = func;
    this.specifiedRowType = specifiedRowType;
  }

  @Override
  public RelDataType deriveRowType() {
    return specifiedRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LogicalMapPartitions(getCluster(), traitSet, sole(inputs), func, specifiedRowType);
  }

  @Override
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    return BeamSqlRelUtils.getNodeStats(getInput(), mq);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    NodeStats inputStat = BeamSqlRelUtils.getNodeStats(getInput(), mq);
    return BeamCostModel.FACTORY.makeCost(inputStat.getRowCount(), inputStat.getRate());
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    throw new UnsupportedOperationException("LogicalMapPartitions cannot be built directly.");
  }
}
