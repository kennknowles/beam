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
import org.apache.beam.sparkconnect.beamrel.BeamHtmlString;
import org.apache.beam.sparkconnect.rel.LogicalHtmlString;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.convert.ConverterRule;

public class BeamHtmlStringRule extends ConverterRule {
  public static final BeamHtmlStringRule INSTANCE = new BeamHtmlStringRule();

  private BeamHtmlStringRule() {
    super(
        LogicalHtmlString.class,
        Convention.NONE,
        BeamLogicalConvention.INSTANCE,
        "BeamHtmlStringRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalHtmlString htmlString = (LogicalHtmlString) rel;
    RelNode input = htmlString.getInput();

    return new BeamHtmlString(
        htmlString.getCluster(),
        htmlString.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule.convert(
            input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        htmlString.numRows,
        htmlString.truncate);
  }
}
