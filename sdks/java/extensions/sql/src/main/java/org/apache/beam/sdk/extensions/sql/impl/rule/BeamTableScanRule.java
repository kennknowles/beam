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
package org.apache.beam.sdk.extensions.sql.impl.rule;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamTableScanRel;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_41_0.org.apache.calcite.rel.logical.LogicalTableScan;

/**
 * This is the conveter rule that converts a Calcite {@code TableFunctionScan} to Beam {@code
 * TableFunctionScanRel}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamTableScanRule extends ConverterRule {
  public static final BeamTableScanRule INSTANCE = new BeamTableScanRule();

  private BeamTableScanRule() {
    super(
        LogicalTableScan.class,
        Convention.NONE,
        BeamLogicalConvention.INSTANCE,
        "BeamTableScanRule");
  }

  @Override
  public RelNode convert(RelNode relNode) {
    TableScan tableScan = (TableScan) relNode;
    checkArgument(
        relNode.getInputs().isEmpty(),
        "Wrong number of inputs for %s, expected 0 inputs but received: %s",
        BeamTableScanRel.class.getSimpleName(),
        relNode.getInputs().size());

    return new BeamTableScanRel(
        tableScan.getCluster(),
        tableScan.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        tableScan.getHints(),
        tableScan.getTable());
  }
}
