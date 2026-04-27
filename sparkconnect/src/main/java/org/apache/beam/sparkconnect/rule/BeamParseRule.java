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
import org.apache.beam.sparkconnect.beamrel.BeamParse;
import org.apache.beam.sparkconnect.rel.LogicalParse;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.convert.ConverterRule;

public class BeamParseRule extends ConverterRule {
  public static final BeamParseRule INSTANCE = new BeamParseRule();

  private BeamParseRule() {
    super(LogicalParse.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamParseRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalParse parse = (LogicalParse) rel;
    RelNode input = parse.getInput();

    return new BeamParse(
        parse.getCluster(),
        parse.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule.convert(
            input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        parse.format,
        parse.beamSchema);
  }
}
