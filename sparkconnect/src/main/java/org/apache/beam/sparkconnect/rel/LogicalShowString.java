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
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** A custom {@code RelNode} to represent the Spark {@code showString} operation. */
public class LogicalShowString extends SingleRel {

  public final int numRows;
  public final int truncate;
  public final boolean vertical;

  public LogicalShowString(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      int numRows,
      int truncate,
      boolean vertical) {
    super(cluster, traits, input);
    this.numRows = numRows;
    this.truncate = truncate;
    this.vertical = vertical;
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
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LogicalShowString(getCluster(), traitSet, sole(inputs), numRows, truncate, vertical);
  }
}
