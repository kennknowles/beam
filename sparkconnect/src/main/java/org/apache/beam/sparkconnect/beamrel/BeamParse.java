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
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;

public class BeamParse extends SingleRel implements BeamRelNode {

  public final org.apache.spark.connect.proto.Parse.ParseFormat format;
  public final Schema beamSchema;

  public BeamParse(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      org.apache.spark.connect.proto.Parse.ParseFormat format,
      Schema beamSchema) {
    super(cluster, traits, input);
    this.format = format;
    this.beamSchema = beamSchema;
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new PTransform<PCollectionList<Row>, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PCollectionList<Row> pinput) {
        checkArgument(
            pinput.size() == 1,
            "Wrong number of inputs for %s: %s",
            BeamParse.class.getSimpleName(),
            pinput);
        PCollection<Row> upstream = pinput.get(0);

        // 1. Extract the string field (assume first field if not specified)
        PCollection<String> jsonStrings =
            upstream.apply(
                "ExtractJsonString",
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        (Row row) -> {
                          String s = row.getString(0);
                          if (s != null) {
                            return s;
                          } else {
                            throw new IllegalArgumentException("JSON string cannot be null");
                          }
                        }));

        // 2. Convert to Row using JsonToRow
        return jsonStrings.apply(JsonToRow.withSchema(beamSchema));
      }
    };
  }

  @Override
  public RelDataType deriveRowType() {
    return CalciteUtils.toCalciteRowType(beamSchema, getCluster().getTypeFactory());
  }

  @Override
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    return NodeStats.create(mq.getRowCount(input), 1, 1);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    return BeamCostModel.FACTORY.makeTinyCost();
  }

  @Override
  public final BeamParse copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BeamParse(getCluster(), traitSet, sole(inputs), format, beamSchema);
  }
}
