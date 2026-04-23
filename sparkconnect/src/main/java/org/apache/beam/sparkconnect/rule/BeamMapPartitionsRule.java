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
package org.apache.beam.sparkconnect.rule;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sparkconnect.beamrel.BeamMapPartitions;
import org.apache.beam.sparkconnect.rel.LogicalMapPartitions;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.convert.ConverterRule;

public class BeamMapPartitionsRule extends ConverterRule {
  public static final BeamMapPartitionsRule INSTANCE = new BeamMapPartitionsRule();

  private BeamMapPartitionsRule() {
    super(
        LogicalMapPartitions.class,
        Convention.NONE,
        BeamLogicalConvention.INSTANCE,
        "BeamMapPartitionsRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalMapPartitions mapPartitions = (LogicalMapPartitions) rel;
    RelNode input = mapPartitions.getInput();

    return new BeamMapPartitions(
        mapPartitions.getCluster(),
        mapPartitions.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        mapPartitions.func,
        mapPartitions.deriveRowType());
  }
}
