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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sparkconnect.ptransform.ShowStringPTransform;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class BeamShowString extends SingleRel implements BeamRelNode {

  private final int numRows;
  private final int truncate;

  public BeamShowString(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, int numRows, int truncate) {
    super(cluster, traits, input);

    this.numRows = numRows;
    this.truncate = truncate;
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new PTransform<PCollectionList<Row>, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PCollectionList<Row> pinput) {
        checkArgument(
            pinput.size() == 1,
            "Wrong number of inputs for %s: %s",
            BeamShowString.class.getSimpleName(),
            pinput);
        PCollection<Row> upstream = pinput.get(0);

        return upstream.apply(new ShowStringPTransform(numRows, truncate));
      }
    };
  }

  @Override
  public RelDataType deriveRowType() {
    // showString produces a single string output.
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    return typeFactory.createStructType(
        ImmutableList.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        ImmutableList.of("show_string"));
  }

  @Override
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    // ShowString is a sink-like operation that materializes a small part of the data.
    // The number of output rows is always 1.
    return NodeStats.create(1, 1, 1);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    // The cost is primarily the cost of pulling `numRows` to the client and formatting them.
    // We can model this as a small constant cost.
    return BeamCostModel.FACTORY.makeTinyCost();
  }

  @Override
  public final BeamShowString copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BeamShowString(getCluster(), traitSet, sole(inputs), numRows, truncate);
  }
}
