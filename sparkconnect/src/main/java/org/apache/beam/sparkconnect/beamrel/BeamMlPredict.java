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
import org.apache.beam.sparkconnect.SparkMLObjectRegistry;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.spark.connect.proto.MlParams;

public class BeamMlPredict extends SingleRel implements BeamRelNode {
  private final SparkMLObjectRegistry.ObjectRefState modelState;
  private final MlParams params;

  public BeamMlPredict(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      SparkMLObjectRegistry.ObjectRefState modelState,
      MlParams params) {
    super(cluster, traits, input);
    this.modelState = modelState;
    this.params = params;
  }

  @Override
  public RelDataType deriveRowType() {
    return getInput().getRowType();
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    throw new UnsupportedOperationException(
        "ML Predict stateful model '"
            + modelState.getOperatorName()
            + "' is not yet supported in execution.");
  }

  @Override
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    return mq.getNodeStats(getInput());
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    return BeamCostModel.FACTORY.makeTinyCost();
  }

  @Override
  public BeamMlPredict copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BeamMlPredict(getCluster(), traitSet, sole(inputs), modelState, params);
  }

  public SparkMLObjectRegistry.ObjectRefState getModelState() {
    return modelState;
  }
}
