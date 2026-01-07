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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import io.substrait.proto.Plan;
import org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the behavior of {@code CalciteQueryPlanner}. Note that this is not for the JDBC path. It
 * will be the behavior of SQLTransform path.
 */
public class CalciteQueryPlannerTest extends BaseRelTest {
  private static final Plan PLAN;

  static {
    try {
      // select * from medium_table
      PLAN =
          Plan.parseFrom(
              new byte[] {
                26, 122, 18, 120, 10, 88, 10, 86, 18, 52, 10, 13, 117, 110, 98, 111, 117, 110, 100,
                101, 100, 95, 107, 101, 121, 10, 9, 108, 97, 114, 103, 101, 95, 107, 101, 121, 10,
                2, 105, 100, 18, 20, 10, 4, 42, 2, 16, 2, 10, 4, 42, 2, 16, 2, 10, 4, 42, 2, 16, 2,
                24, 2, 34, 14, 10, 10, 10, 0, 10, 2, 8, 1, 10, 2, 8, 2, 16, 1, 58, 14, 10, 12, 109,
                101, 100, 105, 117, 109, 95, 116, 97, 98, 108, 101, 18, 13, 117, 110, 98, 111, 117,
                110, 100, 101, 100, 95, 107, 101, 121, 18, 9, 108, 97, 114, 103, 101, 95, 107, 101,
                121, 18, 2, 105, 100, 50, 10, 16, 53, 42, 6, 68, 117, 99, 107, 68, 66
              });
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Before
  public void prepare() {
    registerTable(
        "medium_table",
        TestBoundedTable.of(
                Schema.FieldType.INT32, "unbounded_key",
                Schema.FieldType.INT32, "large_key",
                Schema.FieldType.INT32, "id")
            .addRows(1, 1, 1, 1, 1, 2, 1, 1, 3, 1, 1, 4, 1, 1, 5));
  }

  @Test
  public void testclusterCostHandlerUsesBeamCost() {
    String sql = "select * from medium_table";
    BeamRelNode root = env.parseQuery(sql);
    Assert.assertTrue(
        root.getCluster().getPlanner().getCost(root, root.getCluster().getMetadataQuery())
            instanceof BeamCostModel);
  }

  @Test
  public void testNonCumulativeCostMetadataHandler() {
    String sql = "select * from medium_table";
    BeamRelNode root = env.parseQuery(sql);
    Assert.assertTrue(
        root.getCluster().getMetadataQuery().getNonCumulativeCost(root) instanceof BeamCostModel);
    Assert.assertFalse(
        root.getCluster().getMetadataQuery().getNonCumulativeCost(root).isInfinite());
  }

  @Test
  public void testCumulativeCostMetaDataHandler() {
    // This handler is not our handler. It tests if the cumulative handler of Calcite works as
    // expected.
    String sql = "select * from medium_table";
    BeamRelNode root = env.parseQuery(sql);
    Assert.assertTrue(
        root.getCluster().getMetadataQuery().getCumulativeCost(root) instanceof BeamCostModel);
    Assert.assertFalse(root.getCluster().getMetadataQuery().getCumulativeCost(root).isInfinite());
  }

  @Test
  public void testclusterCostHandlerUsesBeamCostWithSubstrait() {
    BeamRelNode root = env.parsePlan(PLAN);
    Assert.assertTrue(
        root.getCluster().getPlanner().getCost(root, root.getCluster().getMetadataQuery())
            instanceof BeamCostModel);
  }

  @Test
  public void testNonCumulativeCostMetadataHandlerWithSubstrait() {
    BeamRelNode root = env.parsePlan(PLAN);
    Assert.assertTrue(
        root.getCluster().getMetadataQuery().getNonCumulativeCost(root) instanceof BeamCostModel);
    Assert.assertFalse(
        root.getCluster().getMetadataQuery().getNonCumulativeCost(root).isInfinite());
  }

  @Test
  public void testCumulativeCostMetaDataHandlerWithSubstrait() {
    // This handler is not our handler. It tests if the cumulative handler of Calcite works as
    // expected.
    BeamRelNode root = env.parsePlan(PLAN);
    Assert.assertTrue(
        root.getCluster().getMetadataQuery().getCumulativeCost(root) instanceof BeamCostModel);
    Assert.assertFalse(root.getCluster().getMetadataQuery().getCumulativeCost(root).isInfinite());
  }
}
