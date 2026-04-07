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
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.AbstractRelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;

/** A custom {@code RelNode} to represent a Spark {@code LocalRelation} with its data. */
public class SparkLocalRelation extends AbstractRelNode {

  private final List<Row> rows;
  private final RelDataType relDataType;

  public SparkLocalRelation(
      RelOptCluster cluster, RelTraitSet traitSet, List<Row> rows, RelDataType relDataType) {
    super(cluster, traitSet);
    this.rows = rows;
    this.relDataType = relDataType;
  }

  @Override
  public RelDataType deriveRowType() {
    return relDataType;
  }

  public List<Row> getRows() {
    return rows;
  }
}
