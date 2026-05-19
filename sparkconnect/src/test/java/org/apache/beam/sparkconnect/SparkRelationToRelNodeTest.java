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
package org.apache.beam.sparkconnect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Any;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sparkconnect.beamrel.BeamMlFeature;
import org.apache.beam.sparkconnect.beamrel.BeamMlPredict;
import org.apache.beam.sparkconnect.beamrel.BeamShowString;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.spark.connect.proto.Fetch;
import org.apache.spark.connect.proto.MlCommand;
import org.apache.spark.connect.proto.MlOperator;
import org.apache.spark.connect.proto.MlRelation;
import org.apache.spark.connect.proto.ObjectRef;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SQL;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SparkRelationToRelNodeTest {

  private BeamSqlEnv sqlEnv;
  private SparkRelationToRelNode translator;

  @Before
  public void setUp() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    catalogManager.registerTableProvider(
        new org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider());
    BeamSqlEnv.BeamSqlEnvBuilder sqlEnvBuilder = BeamSqlEnv.builder(catalogManager);
    sqlEnvBuilder.setQueryPlannerClassName(CalciteQueryPlanner.class.getCanonicalName());
    sqlEnvBuilder.setPipelineOptions(org.apache.beam.sdk.options.PipelineOptionsFactory.create());

    List<org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule> allRules =
        new java.util.ArrayList<>();
    allRules.addAll(org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets.getAllRules());
    allRules.add(org.apache.beam.sparkconnect.rule.BeamParseRule.INSTANCE);
    allRules.add(org.apache.beam.sparkconnect.rule.BeamMapPartitionsRule.INSTANCE);

    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSet combinedRuleSet =
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSets.ofList(allRules);

    sqlEnvBuilder.setRuleSets(java.util.Collections.singletonList(combinedRuleSet));

    sqlEnv = sqlEnvBuilder.build();

    translator = new SparkRelationToRelNode(sqlEnv, Collections.emptyMap());
  }

  private List<Row> executeRelNode(RelNode relNode) {
    BeamRelNode beamRelNode = sqlEnv.convertToBeamRel(relNode);
    return BeamEnumerableConverter.toRowList(beamRelNode);
  }

  /**
   * Tests the execution of a SQL query. Relevant compliance test: test_sql in
   * python/pyspark/sql/tests/connect/test_connect_basic.py
   */
  @Test
  public void testSql() {
    Relation relation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests a simple project operation. Relevant compliance test: test_simple_project in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Test
  public void testProject() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression idExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("id"))
            .build();

    org.apache.spark.connect.proto.Project project =
        org.apache.spark.connect.proto.Project.newBuilder()
            .setInput(inputRelation)
            .addExpressions(idExpr)
            .build();

    Relation relation = Relation.newBuilder().setProject(project).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32(0).intValue());
  }

  /**
   * Tests a filter operation (Filter). Relevant compliance test: test_filter in
   * python/pyspark/pandas/tests/frame/test_reindexing.py
   */
  @Test
  public void testFilter() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression condition =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedFunction(
                org.apache.spark.connect.proto.Expression.UnresolvedFunction.newBuilder()
                    .setFunctionName(">")
                    .addArguments(
                        org.apache.spark.connect.proto.Expression.newBuilder()
                            .setUnresolvedAttribute(
                                org.apache.spark.connect.proto.Expression.UnresolvedAttribute
                                    .newBuilder()
                                    .setUnparsedIdentifier("id"))
                            .build())
                    .addArguments(
                        org.apache.spark.connect.proto.Expression.newBuilder()
                            .setLiteral(
                                org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                                    .setInteger(1)
                                    .build())
                            .build())
                    .build())
            .build();

    org.apache.spark.connect.proto.Filter filter =
        org.apache.spark.connect.proto.Filter.newBuilder()
            .setInput(inputRelation)
            .setCondition(condition)
            .build();

    Relation relation = Relation.newBuilder().setFilter(filter).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(2, rows.get(0).getInt32("id").intValue());
    assertEquals("b", rows.get(0).getString("name"));
  }

  /**
   * Tests a join operation using column names. Relevant compliance test: test_join_using_columns in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Ignore("Fails during planning or execution of join with SQL inputs.")
  @Test
  public void testJoin() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 20 AS age"))
            .build();

    org.apache.spark.connect.proto.Join join =
        org.apache.spark.connect.proto.Join.newBuilder()
            .setLeft(leftRelation)
            .setRight(rightRelation)
            .addUsingColumns("id")
            .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
            .build();

    Relation relation = Relation.newBuilder().setJoin(join).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
    assertEquals(20, rows.get(0).getInt32("age").intValue());
  }

  @Test
  public void testJoinUsingColumnsSingle() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 20 AS age"))
            .build();

    org.apache.spark.connect.proto.Join join =
        org.apache.spark.connect.proto.Join.newBuilder()
            .setLeft(leftRelation)
            .setRight(rightRelation)
            .addUsingColumns("id")
            .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
            .build();

    Relation relation = Relation.newBuilder().setJoin(join).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    assertTrue(
        relNode
            instanceof
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin);
    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin joinRel =
        (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin) relNode;

    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode condition =
        joinRel.getCondition();
    assertTrue(
        condition instanceof org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall);
    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall call =
        (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall) condition;
    assertEquals(
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind.EQUALS,
        call.getKind());
  }

  @Test
  public void testJoinUsingColumnsList() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name, 10 AS age"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name, 20 AS weight"))
            .build();

    org.apache.spark.connect.proto.Join join =
        org.apache.spark.connect.proto.Join.newBuilder()
            .setLeft(leftRelation)
            .setRight(rightRelation)
            .addUsingColumns("id")
            .addUsingColumns("name")
            .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
            .build();

    Relation relation = Relation.newBuilder().setJoin(join).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    assertTrue(
        relNode
            instanceof
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin);
    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin joinRel =
        (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin) relNode;

    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode condition =
        joinRel.getCondition();
    assertTrue(
        condition instanceof org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall);
    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall call =
        (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall) condition;
    assertEquals(
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind.AND, call.getKind());
    assertEquals(2, call.getOperands().size());
    assertEquals(
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind.EQUALS,
        call.getOperands().get(0).getKind());
    assertEquals(
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind.EQUALS,
        call.getOperands().get(1).getKind());
  }

  @Test
  public void testUnionPlan() {
    Relation leftRelation =
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery("SELECT 1 AS id")).build();

    Relation rightRelation =
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery("SELECT 2 AS id")).build();

    org.apache.spark.connect.proto.SetOperation setOp =
        org.apache.spark.connect.proto.SetOperation.newBuilder()
            .setLeftInput(leftRelation)
            .setRightInput(rightRelation)
            .setSetOpType(org.apache.spark.connect.proto.SetOperation.SetOpType.SET_OP_TYPE_UNION)
            .setIsAll(true)
            .build();

    Relation relation = Relation.newBuilder().setSetOp(setOp).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    assertTrue(
        relNode
            instanceof
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalUnion);
    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalUnion unionRel =
        (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalUnion)
            relNode;

    assertTrue(unionRel.all);
  }

  @Ignore("by_name for UNION is not supported yet")
  @Test
  public void testUnionByNamePlan() {
    Relation leftRelation =
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery("SELECT 1 AS id")).build();

    Relation rightRelation =
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery("SELECT 2 AS id")).build();

    org.apache.spark.connect.proto.SetOperation setOp =
        org.apache.spark.connect.proto.SetOperation.newBuilder()
            .setLeftInput(leftRelation)
            .setRightInput(rightRelation)
            .setSetOpType(org.apache.spark.connect.proto.SetOperation.SetOpType.SET_OP_TYPE_UNION)
            .setIsAll(true)
            .setByName(true)
            .build();

    Relation relation = Relation.newBuilder().setSetOp(setOp).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    assertTrue(
        relNode
            instanceof
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalUnion);
    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalUnion unionRel =
        (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalUnion)
            relNode;

    assertTrue(unionRel.all);
  }

  @Ignore("Sort is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testSortPlan() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression idExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("id"))
            .build();

    org.apache.spark.connect.proto.Expression.SortOrder sortOrder =
        org.apache.spark.connect.proto.Expression.SortOrder.newBuilder()
            .setChild(idExpr)
            .setDirection(
                org.apache.spark.connect.proto.Expression.SortOrder.SortDirection
                    .SORT_DIRECTION_ASCENDING)
            .build();

    org.apache.spark.connect.proto.Sort sort =
        org.apache.spark.connect.proto.Sort.newBuilder()
            .setInput(inputRelation)
            .addOrder(sortOrder)
            .build();

    Relation relation = Relation.newBuilder().setSort(sort).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    assertTrue(
        relNode
            instanceof
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalSort);
  }

  @Ignore("Sort is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testOrderByPlan() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression idExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("id"))
            .build();

    org.apache.spark.connect.proto.Expression.SortOrder sortOrder =
        org.apache.spark.connect.proto.Expression.SortOrder.newBuilder()
            .setChild(idExpr)
            .setDirection(
                org.apache.spark.connect.proto.Expression.SortOrder.SortDirection
                    .SORT_DIRECTION_ASCENDING)
            .build();

    org.apache.spark.connect.proto.Sort sort =
        org.apache.spark.connect.proto.Sort.newBuilder()
            .setInput(inputRelation)
            .addOrder(sortOrder)
            .build();

    Relation relation = Relation.newBuilder().setSort(sort).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    assertTrue(
        relNode
            instanceof
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalSort);
  }

  @Ignore("Hint is currently a no-op and Join fails with SQL inputs")
  @Test
  public void testHintBroadcast() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 20 AS age"))
            .build();

    org.apache.spark.connect.proto.Hint hint =
        org.apache.spark.connect.proto.Hint.newBuilder()
            .setInput(rightRelation)
            .setName("broadcast")
            .build();

    Relation rightWithHint = Relation.newBuilder().setHint(hint).build();

    org.apache.spark.connect.proto.Join join =
        org.apache.spark.connect.proto.Join.newBuilder()
            .setLeft(leftRelation)
            .setRight(rightWithHint)
            .addUsingColumns("id")
            .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
            .build();

    Relation relation = Relation.newBuilder().setJoin(join).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);
  }

  /**
   * Tests a set operation (UNION). Relevant compliance test: test_union in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Ignore("Fails during execution of UNION operation with SQL inputs.")
  @Test
  public void testSetOp() {
    Relation leftRelation =
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery("SELECT 1 AS id")).build();

    Relation rightRelation =
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery("SELECT 2 AS id")).build();

    org.apache.spark.connect.proto.SetOperation setOp =
        org.apache.spark.connect.proto.SetOperation.newBuilder()
            .setLeftInput(leftRelation)
            .setRightInput(rightRelation)
            .setSetOpType(org.apache.spark.connect.proto.SetOperation.SetOpType.SET_OP_TYPE_UNION)
            .setIsAll(true)
            .build();

    Relation relation = Relation.newBuilder().setSetOp(setOp).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals(2, rows.get(1).getInt32("id").intValue());
  }

  /**
   * Tests a sort operation. Relevant compliance test: test_sort in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Ignore("Fails to sort correctly, likely due to no-op fallback for Sort relation.")
  @Test
  public void testSort() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery("SELECT * FROM (VALUES (2, 'b'), (1, 'a')) AS t(id, name)"))
            .build();

    org.apache.spark.connect.proto.Expression idExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("id"))
            .build();

    org.apache.spark.connect.proto.Expression.SortOrder sortOrder =
        org.apache.spark.connect.proto.Expression.SortOrder.newBuilder()
            .setChild(idExpr)
            .setDirection(
                org.apache.spark.connect.proto.Expression.SortOrder.SortDirection
                    .SORT_DIRECTION_ASCENDING)
            .build();

    org.apache.spark.connect.proto.Sort sort =
        org.apache.spark.connect.proto.Sort.newBuilder()
            .setInput(inputRelation)
            .addOrder(sortOrder)
            .build();

    org.apache.spark.connect.proto.Limit limit =
        org.apache.spark.connect.proto.Limit.newBuilder()
            .setInput(Relation.newBuilder().setSort(sort).build())
            .setLimit(2)
            .build();

    Relation relation = Relation.newBuilder().setLimit(limit).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
    assertEquals(2, rows.get(1).getInt32("id").intValue());
    assertEquals("b", rows.get(1).getString("name"));
  }

  /**
   * Tests a limit operation. Relevant compliance test: test_limit in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Test
  public void testLimit() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)"))
            .build();

    org.apache.spark.connect.proto.Limit limit =
        org.apache.spark.connect.proto.Limit.newBuilder()
            .setInput(inputRelation)
            .setLimit(1)
            .build();

    Relation relation = Relation.newBuilder().setLimit(limit).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    // We don't assert the exact content because order is non-deterministic without sort,
    // and sort without limit might fail in Beam SQL.
    assertTrue(rows.get(0).getInt32("id") == 1 || rows.get(0).getInt32("id") == 2);
  }

  /**
   * Tests an aggregate operation (GROUP BY). Relevant compliance test: test_aggregate in
   * python/pyspark/pandas/tests/groupby/test_aggregate.py
   */
  @Test
  public void testAggregate() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery(
                        "SELECT 1 AS id, 10 AS val UNION ALL SELECT 1, 20 UNION ALL SELECT 2, 30"))
            .build();

    org.apache.spark.connect.proto.Expression idExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("id"))
            .build();

    org.apache.spark.connect.proto.Expression valExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("val"))
            .build();

    org.apache.spark.connect.proto.Expression sumExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedFunction(
                org.apache.spark.connect.proto.Expression.UnresolvedFunction.newBuilder()
                    .setFunctionName("sum")
                    .addArguments(valExpr))
            .build();

    org.apache.spark.connect.proto.Expression minExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedFunction(
                org.apache.spark.connect.proto.Expression.UnresolvedFunction.newBuilder()
                    .setFunctionName("min")
                    .addArguments(valExpr))
            .build();

    org.apache.spark.connect.proto.Aggregate aggregate =
        org.apache.spark.connect.proto.Aggregate.newBuilder()
            .setInput(inputRelation)
            .setGroupType(org.apache.spark.connect.proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
            .addGroupingExpressions(idExpr)
            .addAggregateExpressions(sumExpr)
            .addAggregateExpressions(minExpr)
            .build();

    Relation relation = Relation.newBuilder().setAggregate(aggregate).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(2, rows.size());

    // Sort rows by grouping key (index 0) for predictable assertions
    rows.sort((a, b) -> a.getInt32(0).compareTo(b.getInt32(0)));

    // Group id=1: sum=30, min=10
    assertEquals(1, rows.get(0).getInt32(0).intValue());
    assertEquals(30L, ((Number) rows.get(0).getValue(1)).longValue());
    assertEquals(10, ((Number) rows.get(0).getValue(2)).intValue());

    // Group id=2: sum=30, min=30
    assertEquals(2, rows.get(1).getInt32(0).intValue());
    assertEquals(30L, ((Number) rows.get(1).getValue(1)).longValue());
    assertEquals(30, ((Number) rows.get(1).getValue(2)).intValue());
  }

  /**
   * Tests a local relation with schema only. Relevant compliance tests:
   * test_createDataFrame_arrow_* in python/pyspark/sql/tests/connect/arrow/test_parity_arrow.py
   */
  @Test
  public void testLocalRelation() {
    org.apache.spark.connect.proto.LocalRelation localRel =
        org.apache.spark.connect.proto.LocalRelation.newBuilder()
            .setSchema("id INT, name STRING")
            .build();

    Relation relation = Relation.newBuilder().setLocalRelation(localRel).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size()); // Schema only, no data
  }

  /**
   * Tests sampling (Sample). Relevant compliance test: test_sample in
   * python/pyspark/sql/tests/test_dataframe.py
   */
  @Test
  public void testSample() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery(
                        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) AS t(id, name)"))
            .build();

    org.apache.spark.connect.proto.Sample sample =
        org.apache.spark.connect.proto.Sample.newBuilder()
            .setInput(inputRelation)
            .setLowerBound(0.0)
            .setUpperBound(0.5)
            .setWithReplacement(false)
            .build();

    Relation relation = Relation.newBuilder().setSample(sample).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    // With 5 rows and 0.5 fraction, we expect roughly 2-3 rows.
    // Since it's a no-op, it will return all 5 rows, failing this assertion.
    assertTrue("Expected less than 5 rows, got " + rows.size(), rows.size() < 5);
  }

  /**
   * Tests an offset operation. Relevant compliance test: test_offset in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Test
  public void testOffset() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)"))
            .build();

    org.apache.spark.connect.proto.Offset offset =
        org.apache.spark.connect.proto.Offset.newBuilder()
            .setInput(inputRelation)
            .setOffset(1)
            .build();

    Relation relation = Relation.newBuilder().setOffset(offset).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
  }

  /**
   * Tests a deduplicate operation (distinct). Relevant compliance test: test_deduplicate in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Test
  public void testDeduplicate() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery("SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, 'b')) AS t(id, name)"))
            .build();

    org.apache.spark.connect.proto.Deduplicate deduplicate =
        org.apache.spark.connect.proto.Deduplicate.newBuilder()
            .setInput(inputRelation)
            .setAllColumnsAsKeys(true)
            .build();

    Relation relation = Relation.newBuilder().setDeduplicate(deduplicate).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(2, rows.size());

    // Sort rows for predictable assertions
    rows.sort((a, b) -> a.getInt32("id").compareTo(b.getInt32("id")));

    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
    assertEquals(2, rows.get(1).getInt32("id").intValue());
    assertEquals("b", rows.get(1).getString("name"));
  }

  /**
   * Tests a range operation. Relevant compliance test: test_range in
   * python/pyspark/sql/tests/test_dataframe.py
   */
  @Test
  public void testRange() {
    // Case 1: range(1, 1) -> count 0
    org.apache.spark.connect.proto.Range range1 =
        org.apache.spark.connect.proto.Range.newBuilder().setStart(1).setEnd(1).setStep(1).build();
    Relation relation1 = Relation.newBuilder().setRange(range1).build();
    RelNode relNode1 = translator.translate(relation1);
    assertEquals(0, executeRelNode(relNode1).size());

    // Case 2: range(1, 0, -1) -> count 1
    org.apache.spark.connect.proto.Range range2 =
        org.apache.spark.connect.proto.Range.newBuilder().setStart(1).setEnd(0).setStep(-1).build();
    Relation relation2 = Relation.newBuilder().setRange(range2).build();
    RelNode relNode2 = translator.translate(relation2);
    assertEquals(1, executeRelNode(relNode2).size());

    // Case 3: range(0, 1 << 40, 1 << 39) -> count 2
    long end = 1L << 40;
    long step = 1L << 39;
    org.apache.spark.connect.proto.Range range3 =
        org.apache.spark.connect.proto.Range.newBuilder()
            .setStart(0)
            .setEnd(end)
            .setStep(step)
            .build();
    Relation relation3 = Relation.newBuilder().setRange(range3).build();
    RelNode relNode3 = translator.translate(relation3);
    assertEquals(2, executeRelNode(relNode3).size());

    // Case 4: regular range(0, 10, 1)
    org.apache.spark.connect.proto.Range range4 =
        org.apache.spark.connect.proto.Range.newBuilder().setStart(0).setEnd(10).setStep(1).build();
    Relation relation4 = Relation.newBuilder().setRange(range4).build();
    RelNode relNode4 = translator.translate(relation4);
    List<Row> rows = executeRelNode(relNode4);
    assertEquals(10, rows.size());
    for (int i = 0; i < 10; i++) {
      assertEquals((long) i, rows.get(i).getInt64(0).longValue());
    }
  }

  /**
   * Tests a subquery alias operation. Relevant compliance test: test_subquery_alias in
   * python/pyspark/sql/tests/connect/test_connect_basic.py
   */
  @Test
  public void testSubqueryAlias() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.SubqueryAlias subqueryAlias =
        org.apache.spark.connect.proto.SubqueryAlias.newBuilder()
            .setInput(inputRelation)
            .setAlias("my_alias")
            .build();

    Relation relation = Relation.newBuilder().setSubqueryAlias(subqueryAlias).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests a repartition operation. Relevant compliance test: test_coalesce_and_repartition in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Test
  public void testRepartition() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Repartition repartition =
        org.apache.spark.connect.proto.Repartition.newBuilder()
            .setInput(inputRelation)
            .setNumPartitions(2)
            .build();

    Relation relation = Relation.newBuilder().setRepartition(repartition).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests renaming columns using toDF. Relevant compliance test: test_toDF in
   * python/pyspark/sql/tests/connect/test_connect_basic.py
   */
  @Test
  public void testToDF() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.ToDF toDF =
        org.apache.spark.connect.proto.ToDF.newBuilder()
            .setInput(inputRelation)
            .addColumnNames("new_id")
            .addColumnNames("new_name")
            .build();

    Relation relation = Relation.newBuilder().setToDf(toDF).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    Row row = rows.get(0);
    assertEquals(1, row.getInt32(0).intValue());
    assertEquals("a", row.getString(1));

    // Verify column names are updated
    assertEquals("new_id", row.getSchema().getFieldNames().get(0));
    assertEquals("new_name", row.getSchema().getFieldNames().get(1));
  }

  /**
   * Tests renaming columns using withColumnsRenamed. Relevant compliance test:
   * test_with_columns_renamed in python/pyspark/sql/tests/connect/test_connect_basic.py
   */
  @Test
  public void testWithColumnsRenamed() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.WithColumnsRenamed withColumnsRenamed =
        org.apache.spark.connect.proto.WithColumnsRenamed.newBuilder()
            .setInput(inputRelation)
            .addRenames(
                org.apache.spark.connect.proto.WithColumnsRenamed.Rename.newBuilder()
                    .setColName("id")
                    .setNewColName("new_id"))
            .build();

    Relation relation = Relation.newBuilder().setWithColumnsRenamed(withColumnsRenamed).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    Row row = rows.get(0);
    assertEquals(1, row.getInt32(0).intValue());
    assertEquals("a", row.getString(1));

    // Verify column names are updated
    assertEquals("new_id", row.getSchema().getFieldNames().get(0));
    assertEquals("name", row.getSchema().getFieldNames().get(1));
  }

  @Test
  public void testShowStringPhysicalRelTranslation() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.ShowString showString =
        org.apache.spark.connect.proto.ShowString.newBuilder()
            .setInput(inputRelation)
            .setNumRows(5)
            .setTruncate(15)
            .setVertical(false)
            .build();

    Relation relation = Relation.newBuilder().setShowString(showString).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);
    assertTrue(relNode instanceof BeamShowString);

    BeamShowString physicalNode = (BeamShowString) relNode;
    assertEquals(5, physicalNode.getNumRows());
    assertEquals(15, physicalNode.getTruncate());
    assertNotNull(physicalNode.getInput());
  }

  /**
   * Tests a show string operation. Relevant compliance test: test_show in
   * python/pyspark/sql/tests/connect/test_connect_basic.py
   */
  @Test
  public void testShowString() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.ShowString showString =
        org.apache.spark.connect.proto.ShowString.newBuilder()
            .setInput(inputRelation)
            .setNumRows(1)
            .setTruncate(20)
            .setVertical(false)
            .build();

    Relation relation = Relation.newBuilder().setShowString(showString).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    String output = rows.get(0).getString(0);
    String expected =
        "+----+------+\n"
            + "| id | name |\n"
            + "+----+------+\n"
            + "| 1  | a    |\n"
            + "+----+------+\n";
    assertEquals(expected, output);
  }

  @Test
  public void testShowStringWithLocalRelation() {
    org.apache.spark.connect.proto.LocalRelation localRel =
        org.apache.spark.connect.proto.LocalRelation.newBuilder()
            .setSchema("id INT, name STRING")
            .build();
    Relation inputRelation = Relation.newBuilder().setLocalRelation(localRel).build();

    org.apache.spark.connect.proto.ShowString showString =
        org.apache.spark.connect.proto.ShowString.newBuilder()
            .setInput(inputRelation)
            .setNumRows(1)
            .setTruncate(20)
            .setVertical(false)
            .build();

    Relation relation = Relation.newBuilder().setShowString(showString).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertNotNull(rows);
  }

  /**
   * Tests dropping columns (Drop). Relevant compliance test: test_drop in
   * python/pyspark/sql/tests/test_dataframe.py
   */
  @Test
  public void testDrop() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name, 'Y' AS active"))
            .build();

    // Case 1: Drop 'active' -> should leave 'id' and 'name'
    org.apache.spark.connect.proto.Drop drop1 =
        org.apache.spark.connect.proto.Drop.newBuilder()
            .setInput(inputRelation)
            .addColumnNames("active")
            .build();

    Relation relation1 = Relation.newBuilder().setDrop(drop1).build();
    RelNode relNode1 = translator.translate(relation1);
    List<Row> rows1 = executeRelNode(relNode1);
    assertEquals(1, rows1.size());
    assertEquals(2, rows1.get(0).getSchema().getFieldCount());
    assertEquals("id", rows1.get(0).getSchema().getFieldNames().get(0));
    assertEquals("name", rows1.get(0).getSchema().getFieldNames().get(1));

    // Case 2: Drop 'active' and a non-existent column -> should still leave 'id' and 'name'
    org.apache.spark.connect.proto.Drop drop2 =
        org.apache.spark.connect.proto.Drop.newBuilder()
            .setInput(inputRelation)
            .addColumnNames("active")
            .addColumnNames("nonexistent")
            .build();

    Relation relation2 = Relation.newBuilder().setDrop(drop2).build();
    RelNode relNode2 = translator.translate(relation2);
    List<Row> rows2 = executeRelNode(relNode2);
    assertEquals(1, rows2.size());
    assertEquals(2, rows2.get(0).getSchema().getFieldCount());

    // Case 3: Drop all columns -> should leave empty schema
    org.apache.spark.connect.proto.Drop drop3 =
        org.apache.spark.connect.proto.Drop.newBuilder()
            .setInput(inputRelation)
            .addColumnNames("id")
            .addColumnNames("name")
            .addColumnNames("active")
            .build();

    Relation relation3 = Relation.newBuilder().setDrop(drop3).build();
    RelNode relNode3 = translator.translate(relation3);
    List<Row> rows3 = executeRelNode(relNode3);
    assertEquals(1, rows3.size());
    assertEquals(0, rows3.get(0).getSchema().getFieldCount());
  }

  /**
   * Tests a tail operation. Relevant compliance test: test_tail in
   * python/pyspark/pandas/tests/connect/groupby/test_parity_head_tail.py
   */
  @Ignore("Fails execution of Tail operation with SQL inputs.")
  @Test
  public void testTail() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)"))
            .build();

    org.apache.spark.connect.proto.Tail tail =
        org.apache.spark.connect.proto.Tail.newBuilder()
            .setInput(inputRelation)
            .setLimit(1)
            .build();

    Relation relation = Relation.newBuilder().setTail(tail).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(2, rows.get(0).getInt32("id").intValue());
    assertEquals("b", rows.get(0).getString("name"));
  }

  /**
   * Tests adding/updating columns using withColumns. Relevant compliance test: test_with_columns in
   * python/pyspark/sql/tests/test_dataframe.py
   */
  @Test
  public void testWithColumns() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    // Case 1: Overwrite existing column 'id' with 10
    org.apache.spark.connect.proto.Expression valExpr1 =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setLiteral(
                org.apache.spark.connect.proto.Expression.Literal.newBuilder().setInteger(10))
            .build();

    org.apache.spark.connect.proto.Expression.Alias alias1 =
        org.apache.spark.connect.proto.Expression.Alias.newBuilder()
            .setExpr(valExpr1)
            .addName("id")
            .build();

    org.apache.spark.connect.proto.WithColumns withColumns1 =
        org.apache.spark.connect.proto.WithColumns.newBuilder()
            .setInput(inputRelation)
            .addAliases(alias1)
            .build();

    Relation relation1 = Relation.newBuilder().setWithColumns(withColumns1).build();
    RelNode relNode1 = translator.translate(relation1);
    List<Row> rows1 = executeRelNode(relNode1);
    assertEquals(1, rows1.size());
    assertEquals(10, rows1.get(0).getInt32("id").intValue());
    assertEquals("a", rows1.get(0).getString("name"));

    // Case 2: Add multiple columns
    org.apache.spark.connect.proto.Expression valExpr2 =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setLiteral(
                org.apache.spark.connect.proto.Expression.Literal.newBuilder().setInteger(2))
            .build();

    org.apache.spark.connect.proto.Expression.Alias alias2 =
        org.apache.spark.connect.proto.Expression.Alias.newBuilder()
            .setExpr(valExpr2)
            .addName("new_col1")
            .build();

    org.apache.spark.connect.proto.Expression.Alias alias3 =
        org.apache.spark.connect.proto.Expression.Alias.newBuilder()
            .setExpr(valExpr2)
            .addName("new_col2")
            .build();

    org.apache.spark.connect.proto.WithColumns withColumns2 =
        org.apache.spark.connect.proto.WithColumns.newBuilder()
            .setInput(inputRelation)
            .addAliases(alias2)
            .addAliases(alias3)
            .build();

    Relation relation2 = Relation.newBuilder().setWithColumns(withColumns2).build();
    RelNode relNode2 = translator.translate(relation2);
    List<Row> rows2 = executeRelNode(relNode2);
    assertEquals(1, rows2.size());
    assertEquals(1, rows2.get(0).getInt32("id").intValue());
    assertEquals(2, rows2.get(0).getInt32("new_col1").intValue());
    assertEquals(2, rows2.get(0).getInt32("new_col2").intValue());
  }

  /**
   * Tests a hint operation. Relevant compliance test: test_hint in
   * python/pyspark/pandas/tests/test_frame_spark.py
   */
  @Ignore("Fails because join fails, and hint is a no-op in SparkRelationToRelNode")
  @Test
  public void testHint() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 20 AS age"))
            .build();

    org.apache.spark.connect.proto.Hint hint =
        org.apache.spark.connect.proto.Hint.newBuilder()
            .setInput(rightRelation)
            .setName("BROADCAST")
            .build();

    Relation hintedRightRelation = Relation.newBuilder().setHint(hint).build();

    org.apache.spark.connect.proto.Join join =
        org.apache.spark.connect.proto.Join.newBuilder()
            .setLeft(leftRelation)
            .setRight(hintedRightRelation)
            .addUsingColumns("id")
            .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
            .build();

    Relation relation = Relation.newBuilder().setJoin(join).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
    assertEquals(20, rows.get(0).getInt32("age").intValue());
  }

  /**
   * Tests an unpivot operation. Relevant compliance test: test_unpivot in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Ignore("Fails execution of Unpivot operation, likely due to no-op fallback.")
  @Test
  public void testUnpivot() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Unpivot unpivot =
        org.apache.spark.connect.proto.Unpivot.newBuilder()
            .setInput(inputRelation)
            .setVariableColumnName("var")
            .setValueColumnName("val")
            .build();

    Relation relation = Relation.newBuilder().setUnpivot(unpivot).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(2, rows.size());

    rows.sort((a, b) -> a.getString(0).compareTo(b.getString(0))); // assuming var is col 0

    assertEquals("id", rows.get(0).getString(0));
    assertEquals("1", rows.get(0).getString(1));

    assertEquals("name", rows.get(1).getString(0));
    assertEquals("a", rows.get(1).getString(1));
  }

  /**
   * Tests applying a schema using toSchema. Relevant compliance test: test_to in
   * python/pyspark/sql/tests/test_dataframe.py
   */
  @Test
  public void testToSchema() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    // Target schema: new_id (STRING)
    // Should rename 'id' to 'new_id' and cast to STRING, and drop 'name' (by position)
    org.apache.spark.connect.proto.DataType.StructField field =
        org.apache.spark.connect.proto.DataType.StructField.newBuilder()
            .setName("new_id")
            .setDataType(
                org.apache.spark.connect.proto.DataType.newBuilder()
                    .setString(org.apache.spark.connect.proto.DataType.String.newBuilder().build()))
            .build();

    org.apache.spark.connect.proto.DataType structType =
        org.apache.spark.connect.proto.DataType.newBuilder()
            .setStruct(org.apache.spark.connect.proto.DataType.Struct.newBuilder().addFields(field))
            .build();

    org.apache.spark.connect.proto.ToSchema toSchema =
        org.apache.spark.connect.proto.ToSchema.newBuilder()
            .setInput(inputRelation)
            .setSchema(structType)
            .build();

    Relation relation = Relation.newBuilder().setToSchema(toSchema).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    Row row = rows.get(0);

    // Verify schema has only 'new_id' of type STRING
    assertEquals(1, row.getSchema().getFieldCount());
    assertEquals("new_id", row.getSchema().getFieldNames().get(0));
    assertEquals("1", row.getString(0)); // 1 cast to string
  }

  /**
   * Tests a repartition by expression operation. Relevant compliance test:
   * test_repartition_by_expression in python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Test
  public void testRepartitionByExpression() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression idExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("id"))
            .build();

    org.apache.spark.connect.proto.RepartitionByExpression repartitionByExpression =
        org.apache.spark.connect.proto.RepartitionByExpression.newBuilder()
            .setInput(inputRelation)
            .addPartitionExprs(idExpr)
            .build();

    Relation relation =
        Relation.newBuilder().setRepartitionByExpression(repartitionByExpression).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests a map partitions operation. Relevant compliance test: test_chain_map_partitions_in_pandas
   * in python/pyspark/sql/tests/connect/pandas/test_parity_pandas_map.py Note: This operation is
   * currently unsupported and expected to throw UnsupportedOperationException.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testMapPartitions() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.MapPartitions mapPartitions1 =
        org.apache.spark.connect.proto.MapPartitions.newBuilder()
            .setInput(inputRelation)
            .setFunc(
                org.apache.spark.connect.proto.CommonInlineUserDefinedFunction.newBuilder()
                    .setFunctionName("my_func1"))
            .build();

    Relation relation1 = Relation.newBuilder().setMapPartitions(mapPartitions1).build();

    org.apache.spark.connect.proto.MapPartitions mapPartitions2 =
        org.apache.spark.connect.proto.MapPartitions.newBuilder()
            .setInput(relation1)
            .setFunc(
                org.apache.spark.connect.proto.CommonInlineUserDefinedFunction.newBuilder()
                    .setFunctionName("my_func2"))
            .build();

    Relation relation2 = Relation.newBuilder().setMapPartitions(mapPartitions2).build();

    // This should throw UnsupportedOperationException during translation or execution of the first
    // MapPartitions
    RelNode relNode = translator.translate(relation2);
    assertNotNull(relNode);
    executeRelNode(relNode);
  }

  /**
   * Tests collecting metrics (observe). Relevant compliance test: test_observe in
   * python/pyspark/sql/tests/test_observation.py
   */
  @Test
  public void testCollectMetrics() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression countExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedFunction(
                org.apache.spark.connect.proto.Expression.UnresolvedFunction.newBuilder()
                    .setFunctionName("count")
                    .addArguments(
                        org.apache.spark.connect.proto.Expression.newBuilder()
                            .setLiteral(
                                org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                                    .setInteger(1)
                                    .build())
                            .build())
                    .build())
            .build();

    org.apache.spark.connect.proto.Expression.Alias alias =
        org.apache.spark.connect.proto.Expression.Alias.newBuilder()
            .setExpr(countExpr)
            .addName("cnt")
            .build();

    org.apache.spark.connect.proto.Expression aliasExpr =
        org.apache.spark.connect.proto.Expression.newBuilder().setAlias(alias).build();

    org.apache.spark.connect.proto.CollectMetrics collectMetrics =
        org.apache.spark.connect.proto.CollectMetrics.newBuilder()
            .setInput(inputRelation)
            .setName("my_metrics")
            .addMetrics(aliasExpr)
            .build();

    Relation relation = Relation.newBuilder().setCollectMetrics(collectMetrics).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());

    // We cannot easily assert on collected metrics here as they are handled out-of-band in Spark
    // Connect.
    // But we expect the translation to succeed and return the input data transparently.
    assertEquals(1, rows.get(0).getInt32("id").intValue());
  }

  /**
   * Tests a parse operation (JSON). Relevant compliance test: test_parse_json in
   * python/pyspark/sql/tests/test_functions.py
   */
  @Test
  public void testParse() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT '{\"id\":1}' AS text"))
            .build();

    org.apache.spark.connect.proto.Parse parse =
        org.apache.spark.connect.proto.Parse.newBuilder()
            .setInput(inputRelation)
            .setFormat(org.apache.spark.connect.proto.Parse.ParseFormat.PARSE_FORMAT_JSON)
            .build();

    Relation relation = Relation.newBuilder().setParse(parse).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    // Result should be a structured row with field 'id' equal to 1.
    // Since it's a no-op, it will return the string, failing this assertion.
    assertEquals(1, rows.get(0).getInt32("id").intValue());
  }

  /**
   * Tests a group map operation (applyInPandas). Relevant compliance test: test_applyInPandas_basic
   * in python/pyspark/sql/tests/connect/pandas/test_parity_pandas_map.py
   */
  @Test
  public void testGroupMap() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.GroupMap groupMap =
        org.apache.spark.connect.proto.GroupMap.newBuilder()
            .setInput(inputRelation)
            .setFunc(
                org.apache.spark.connect.proto.CommonInlineUserDefinedFunction.newBuilder()
                    .setFunctionName("my_func"))
            .build();

    Relation relation = Relation.newBuilder().setGroupMap(groupMap).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests a cogroup map operation (applyInPandas on cogrouped dataframes). Relevant compliance
   * test: test_cogroup_apply_in_pandas_with_logging in
   * python/pyspark/sql/tests/connect/pandas/test_parity_pandas_map.py
   */
  @Test
  public void testCoGroupMap() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    Relation otherRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'b' AS name"))
            .build();

    org.apache.spark.connect.proto.CoGroupMap coGroupMap =
        org.apache.spark.connect.proto.CoGroupMap.newBuilder()
            .setInput(inputRelation)
            .setOther(otherRelation)
            .setFunc(
                org.apache.spark.connect.proto.CommonInlineUserDefinedFunction.newBuilder()
                    .setFunctionName("my_func"))
            .build();

    Relation relation = Relation.newBuilder().setCoGroupMap(coGroupMap).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests adding a watermark to a relation. Relevant compliance test:
   * test_streaming_drop_duplicate_within_watermark in
   * python/pyspark/sql/tests/connect/streaming/test_parity_streaming.py
   */
  @Ignore("Fails during translation of SQL input with timestamp.")
  @Test
  public void testWithWatermark() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery(
                        "SELECT 1 AS id, 'a' AS name, TIMESTAMP '2026-04-24 12:00:00' AS timestamp"))
            .build();

    org.apache.spark.connect.proto.WithWatermark withWatermark =
        org.apache.spark.connect.proto.WithWatermark.newBuilder()
            .setInput(inputRelation)
            .setEventTime("timestamp")
            .setDelayThreshold("10 minutes")
            .build();

    Relation relation = Relation.newBuilder().setWithWatermark(withWatermark).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests a group map with state operation (applyInPandasWithState). Relevant compliance test:
   * test_applyInPandasWithState in
   * python/pyspark/sql/tests/connect/pandas/test_parity_pandas_map.py
   */
  @Test
  public void testApplyInPandasWithState() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.ApplyInPandasWithState applyInPandasWithState =
        org.apache.spark.connect.proto.ApplyInPandasWithState.newBuilder()
            .setInput(inputRelation)
            .setFunc(
                org.apache.spark.connect.proto.CommonInlineUserDefinedFunction.newBuilder()
                    .setFunctionName("my_func"))
            .setOutputSchema("id INT")
            .setStateSchema("state INT")
            .build();

    Relation relation =
        Relation.newBuilder().setApplyInPandasWithState(applyInPandasWithState).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests an HTML string representation operation. Relevant compliance test: test_to_html in
   * python/pyspark/pandas/tests/io/test_dataframe_conversion.py
   */
  @Test
  public void testHtmlString() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.HtmlString htmlString =
        org.apache.spark.connect.proto.HtmlString.newBuilder()
            .setInput(inputRelation)
            .setNumRows(1)
            .setTruncate(20)
            .build();

    Relation relation = Relation.newBuilder().setHtmlString(htmlString).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    // Result should be a single string containing the HTML table.
    // Since it's a no-op, it will return the input row, failing this assertion.
    String html = rows.get(0).getString(0);
    assertTrue("Expected HTML table, got: " + html, html.contains("<table"));
  }

  @Test
  public void testCachedLocalRelation() {
    org.apache.spark.connect.proto.CachedLocalRelation cachedLocalRel =
        org.apache.spark.connect.proto.CachedLocalRelation.newBuilder()
            .setHash("dummy_hash")
            .build();

    Relation relation = Relation.newBuilder().setCachedLocalRelation(cachedLocalRel).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size());
  }

  /**
   * Tests a cached remote relation operation. Relevant compliance test: test_df_caache in
   * python/pyspark/sql/tests/connect/test_connect_basic.py
   */
  @Test
  public void testCachedRemoteRelation() {
    org.apache.spark.connect.proto.CachedRemoteRelation cachedRemoteRel =
        org.apache.spark.connect.proto.CachedRemoteRelation.newBuilder()
            .setRelationId("dummy_id")
            .build();

    Relation relation = Relation.newBuilder().setCachedRemoteRelation(cachedRemoteRel).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size());
  }

  /**
   * Tests a common inline user defined table function operation. Relevant compliance test:
   * test_udtf in python/pyspark/sql/tests/connect/test_connect_function.py
   */
  @Test
  public void testCommonInlineUserDefinedTableFunction() {
    org.apache.spark.connect.proto.CommonInlineUserDefinedTableFunction udtf =
        org.apache.spark.connect.proto.CommonInlineUserDefinedTableFunction.newBuilder()
            .setFunctionName("my_udtf")
            .build();

    Relation relation = Relation.newBuilder().setCommonInlineUserDefinedTableFunction(udtf).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size());
  }

  /**
   * Tests an as-of join operation. Relevant compliance test: test_merge_asof in
   * python/pyspark/pandas/tests/reshape/test_merge_asof.py
   */
  @Ignore("AsOfJoin is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testAsOfJoin() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery(
                        "SELECT 1 AS a, 'a' AS left_val UNION ALL SELECT 5, 'b' UNION ALL SELECT 10, 'c'"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(
                SQL.newBuilder()
                    .setQuery(
                        "SELECT 1 AS a, 10 AS right_val UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 30 UNION ALL SELECT 6, 60 UNION ALL SELECT 7, 70"))
            .build();

    org.apache.spark.connect.proto.Expression aExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("a"))
            .build();

    org.apache.spark.connect.proto.AsOfJoin asOfJoin =
        org.apache.spark.connect.proto.AsOfJoin.newBuilder()
            .setLeft(leftRelation)
            .setRight(rightRelation)
            .setLeftAsOf(aExpr)
            .setRightAsOf(aExpr)
            .build();

    Relation relation = Relation.newBuilder().setAsOfJoin(asOfJoin).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    // We expect 3 rows (matching left relation)
    assertEquals(3, rows.size());

    // Sort rows by 'a' to be sure
    rows.sort((r1, r2) -> r1.getInt32("a").compareTo(r2.getInt32("a")));

    // Row for a=5 should match a=3 from right, so right_val should be 30
    // Since it's a no-op, it will fail because right_val won't even be in the schema
    Row row5 = rows.get(1);
    assertEquals(5, row5.getInt32("a").intValue());
    assertEquals(30, row5.getInt32("right_val").intValue());
  }

  /**
   * Tests a common inline user defined data source operation. Relevant compliance test:
   * test_datasource_read in python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Test
  public void testCommonInlineUserDefinedDataSource() {
    org.apache.spark.connect.proto.CommonInlineUserDefinedDataSource udds =
        org.apache.spark.connect.proto.CommonInlineUserDefinedDataSource.newBuilder()
            .setName("my_udds")
            .build();

    Relation relation = Relation.newBuilder().setCommonInlineUserDefinedDataSource(udds).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size());
  }

  /**
   * Tests a WithRelations container operation. This operation contains a root relation and
   * potentially other dependent relations.
   */
  @Test
  public void testWithRelations() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.WithRelations withRelations =
        org.apache.spark.connect.proto.WithRelations.newBuilder().setRoot(inputRelation).build();

    Relation relation = Relation.newBuilder().setWithRelations(withRelations).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  /**
   * Tests a transpose operation. Relevant compliance test: test_transpose in
   * python/pyspark/pandas/tests/connect/frame/test_parity_reshaping.py
   */
  @Ignore("Fails execution of Transpose operation, likely due to no-op fallback.")
  @Test
  public void testTranspose() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Transpose transpose =
        org.apache.spark.connect.proto.Transpose.newBuilder().setInput(inputRelation).build();

    Relation relation = Relation.newBuilder().setTranspose(transpose).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(2, rows.size());

    rows.sort((a, b) -> a.getString(0).compareTo(b.getString(0)));

    assertEquals("id", rows.get(0).getString(0));
    assertEquals("1", rows.get(0).getString(1));

    assertEquals("name", rows.get(1).getString(0));
    assertEquals("a", rows.get(1).getString(1));
  }

  /**
   * Tests an unresolved table valued function operation. Relevant compliance test:
   * test_lateral_join_with_table_valued_functions in
   * python/pyspark/sql/tests/connect/test_connect_plan.py (or similar advanced tests file)
   */
  @Test
  public void testUnresolvedTableValuedFunction() {
    org.apache.spark.connect.proto.UnresolvedTableValuedFunction utvf =
        org.apache.spark.connect.proto.UnresolvedTableValuedFunction.newBuilder()
            .setFunctionName("my_func")
            .build();

    Relation relation = Relation.newBuilder().setUnresolvedTableValuedFunction(utvf).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size());
  }

  /**
   * Tests a lateral join operation. Relevant compliance test:
   * test_lateral_join_with_single_column_select in
   * python/pyspark/sql/tests/connect/test_connect_plan.py (or similar advanced tests file)
   */
  @Test
  public void testLateralJoin() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'b' AS name"))
            .build();

    org.apache.spark.connect.proto.LateralJoin lateralJoin =
        org.apache.spark.connect.proto.LateralJoin.newBuilder()
            .setLeft(leftRelation)
            .setRight(rightRelation)
            .build();

    Relation relation = Relation.newBuilder().setLateralJoin(lateralJoin).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("a", rows.get(0).getString("name"));
  }

  //  @Test
  //  public void testChunkedCachedLocalRelation() {
  //    org.apache.spark.connect.proto.ChunkedCachedLocalRelation chunkedCachedLocalRel =
  //        org.apache.spark.connect.proto.ChunkedCachedLocalRelation.newBuilder()
  //            .addDataHashes("dummy_hash")
  //            .build();
  //
  //    Relation relation =
  //        Relation.newBuilder().setChunkedCachedLocalRelation(chunkedCachedLocalRel).build();
  //
  //    RelNode relNode = translator.translate(relation);
  //    assertNotNull(relNode);
  //
  //    try {
  //      List<Row> rows = executeRelNode(relNode);
  //    } catch (Exception e) {
  //      // Expected if not fully supported.
  //    }
  //  }
  //
  //  @Test
  //  public void testRelationChanges() {
  //    org.apache.spark.connect.proto.RelationChanges relationChanges =
  //        org.apache.spark.connect.proto.RelationChanges.newBuilder()
  //            .setUnparsedIdentifier("my_table")
  //            .build();
  //
  //    Relation relation = Relation.newBuilder().setRelationChanges(relationChanges).build();
  //
  //    RelNode relNode = translator.translate(relation);
  //    assertNotNull(relNode);
  //
  //    try {
  //      List<Row> rows = executeRelNode(relNode);
  //    } catch (Exception e) {
  //      // Expected if not fully supported.
  //    }
  //  }

  /**
   * Tests filling missing values (NAFill). Relevant compliance test: test_fill_na in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Ignore("Fails to fill null values, likely due to no-op fallback.")
  @Test
  public void testNAFill() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, CAST(NULL AS STRING) AS name"))
            .build();

    org.apache.spark.connect.proto.NAFill naFill =
        org.apache.spark.connect.proto.NAFill.newBuilder()
            .setInput(inputRelation)
            .addValues(
                org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                    .setString("b")
                    .build())
            .build();

    Relation relation = Relation.newBuilder().setFillNa(naFill).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt32("id").intValue());
    assertEquals("b", rows.get(0).getString("name"));
  }

  /**
   * Tests dropping rows with missing values (NADrop). Relevant compliance test: test_drop_na in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Ignore("Fails execution of NADrop operation, likely due to no-op fallback.")
  @Test
  public void testNADrop() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, CAST(NULL AS STRING) AS name"))
            .build();

    org.apache.spark.connect.proto.NADrop naDrop =
        org.apache.spark.connect.proto.NADrop.newBuilder().setInput(inputRelation).build();

    Relation relation = Relation.newBuilder().setDropNa(naDrop).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size());
  }

  /**
   * Tests replacing values (NAReplace). Relevant compliance test: test_replace in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Ignore("NAReplace is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testNAReplace() {
    Relation inputRelation =
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery("SELECT 10.0 AS id")).build();

    org.apache.spark.connect.proto.NAReplace naReplace =
        org.apache.spark.connect.proto.NAReplace.newBuilder()
            .setInput(inputRelation)
            .addReplacements(
                org.apache.spark.connect.proto.NAReplace.Replacement.newBuilder()
                    .setOldValue(
                        org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                            .setDouble(10.0)
                            .build())
                    .setNewValue(
                        org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                            .setDouble(20.0)
                            .build())
                    .build())
            .build();

    Relation relation = Relation.newBuilder().setReplace(naReplace).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals(20.0, rows.get(0).getDouble("id"), 0.001);
  }

  /**
   * Tests a stat summary operation (describe). Relevant compliance test: test_summary in
   * python/pyspark/sql/tests/connect/test_connect_plan.py
   */
  @Ignore("StatSummary is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testStatSummary() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.StatSummary statSummary =
        org.apache.spark.connect.proto.StatSummary.newBuilder()
            .setInput(inputRelation)
            .addStatistics("count")
            .build();

    Relation relation = Relation.newBuilder().setSummary(statSummary).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals("count", rows.get(0).getString("summary"));
    assertEquals("1", rows.get(0).getString("id"));
    assertEquals("1", rows.get(0).getString("name"));
  }

  /**
   * Tests reading from a data source (CSV) with options. Relevant compliance test: test_read_csv in
   * python/pyspark/pandas/tests/io/test_csv.py
   */
  @Test
  public void testRead() {
    org.apache.spark.connect.proto.Read.DataSource dataSource =
        org.apache.spark.connect.proto.Read.DataSource.newBuilder()
            .setFormat("csv")
            .setSchema("id INT, name STRING")
            .addPaths("dummy_path")
            .putOptions("usecols", "id")
            .build();

    org.apache.spark.connect.proto.Read read =
        org.apache.spark.connect.proto.Read.newBuilder().setDataSource(dataSource).build();

    Relation relation = Relation.newBuilder().setRead(read).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    // If usecols worked, we would only have 'id' column.
    // Since it's ignored, it will return both, failing this assertion if we assert schema size 1.
    assertEquals(1, rows.get(0).getSchema().getFieldCount());
    assertEquals("id", rows.get(0).getSchema().getFieldNames().get(0));
  }

  @Ignore("StatCrosstab is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testStatCrosstab() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.StatCrosstab crosstab =
        org.apache.spark.connect.proto.StatCrosstab.newBuilder()
            .setInput(inputRelation)
            .setCol1("id")
            .setCol2("name")
            .build();

    Relation relation = Relation.newBuilder().setCrosstab(crosstab).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    assertEquals("1", rows.get(0).getString("id"));
    assertEquals(1, rows.get(0).getInt32("a").intValue());
  }

  @Ignore("StatDescribe is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testStatDescribe() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.StatDescribe describe =
        org.apache.spark.connect.proto.StatDescribe.newBuilder().setInput(inputRelation).build();

    Relation relation = Relation.newBuilder().setDescribe(describe).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    // Describe returns multiple rows (count, mean, stddev, min, max).
    // We check for the presence of the "count" row.
    assertTrue(rows.size() >= 1);

    // Find the "count" row
    Row countRow = null;
    for (Row row : rows) {
      if ("count".equals(row.getString("summary"))) {
        countRow = row;
        break;
      }
    }

    assertNotNull("Expected 'count' row in describe output", countRow);
    assertEquals("1", countRow.getString("id"));
    assertEquals("1", countRow.getString("name"));
  }

  @Ignore("StatCov is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testStatCov() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1.0 AS id UNION ALL SELECT 2.0 AS id"))
            .build();

    org.apache.spark.connect.proto.StatCov cov =
        org.apache.spark.connect.proto.StatCov.newBuilder()
            .setInput(inputRelation)
            .setCol1("id")
            .setCol2("id")
            .build();

    Relation relation = Relation.newBuilder().setCov(cov).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    // Result should be the covariance value. Spark usually returns it as a Double.
    // Let's assume the first column contains the result.
    assertEquals(0.5, rows.get(0).getDouble(0), 0.001);
  }

  @Ignore("StatCorr is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testStatCorr() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1.0 AS id UNION ALL SELECT 2.0 AS id"))
            .build();

    org.apache.spark.connect.proto.StatCorr corr =
        org.apache.spark.connect.proto.StatCorr.newBuilder()
            .setInput(inputRelation)
            .setCol1("id")
            .setCol2("id")
            .build();

    Relation relation = Relation.newBuilder().setCorr(corr).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    // Result should be the correlation value (1.0 for self-correlation).
    // Let's assume the first column contains the result.
    assertEquals(1.0, rows.get(0).getDouble(0), 0.001);
  }

  @Ignore("StatApproxQuantile is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testStatApproxQuantile() {
    Relation inputRelation =
        Relation.newBuilder().setSql(SQL.newBuilder().setQuery("SELECT 1.0 AS id")).build();

    org.apache.spark.connect.proto.StatApproxQuantile approxQuantile =
        org.apache.spark.connect.proto.StatApproxQuantile.newBuilder()
            .setInput(inputRelation)
            .addCols("id")
            .addProbabilities(0.5)
            .setRelativeError(0.05)
            .build();

    Relation relation = Relation.newBuilder().setApproxQuantile(approxQuantile).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    // Result should be the quantile value (1.0 for a single value).
    // Let's assume the first column contains the result.
    assertEquals(1.0, rows.get(0).getDouble(0), 0.001);
  }

  @Ignore("StatFreqItems is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testStatFreqItems() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.StatFreqItems freqItems =
        org.apache.spark.connect.proto.StatFreqItems.newBuilder()
            .setInput(inputRelation)
            .addCols("id")
            .build();

    Relation relation = Relation.newBuilder().setFreqItems(freqItems).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
    // Result should be a row containing an array of frequent items for 'id'.
    // Let's assume the first column contains the array.
    assertNotNull(rows.get(0).getArray(0));
  }

  @Ignore("StatSampleBy is currently a no-op in SparkRelationToRelNode")
  @Test
  public void testStatSampleBy() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression idExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("id"))
            .build();

    org.apache.spark.connect.proto.StatSampleBy sampleBy =
        org.apache.spark.connect.proto.StatSampleBy.newBuilder()
            .setInput(inputRelation)
            .setCol(idExpr)
            .build();

    Relation relation = Relation.newBuilder().setSampleBy(sampleBy).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    // Assuming fraction is not set, it might return empty or all.
    // Let's assert we get at most 1 row.
    assertTrue(rows.size() <= 1);
  }

  @Test
  public void testMlBinarizerTranslationAndExecution() {
    org.apache.spark.connect.proto.LocalRelation localRel =
        org.apache.spark.connect.proto.LocalRelation.newBuilder()
            .setSchema("id INT, feature DOUBLE")
            .build();
    Relation inputRelation = Relation.newBuilder().setLocalRelation(localRel).build();

    org.apache.spark.connect.proto.MlParams params =
        org.apache.spark.connect.proto.MlParams.newBuilder()
            .putParams(
                "inputCol",
                org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                    .setString("feature")
                    .build())
            .putParams(
                "outputCol",
                org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                    .setString("binarized")
                    .build())
            .putParams(
                "threshold",
                org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                    .setDouble(5.0)
                    .build())
            .build();

    MlRelation.Transform transform =
        MlRelation.Transform.newBuilder()
            .setTransformer(MlOperator.newBuilder().setName("Binarizer").build())
            .setInput(inputRelation)
            .setParams(params)
            .build();

    Any any = Any.pack(transform);
    Relation relation = Relation.newBuilder().setExtension(any).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);
    assertTrue(
        relNode
            instanceof
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject);

    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject
        projectNode =
            (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject)
                relNode;
    assertNotNull(projectNode.getRowType());
    assertEquals(3, projectNode.getRowType().getFieldCount());
    assertEquals("id", projectNode.getRowType().getFieldNames().get(0));
    assertEquals("feature", projectNode.getRowType().getFieldNames().get(1));
    assertEquals("binarized", projectNode.getRowType().getFieldNames().get(2));

    // Verify it compiles and executes cleanly on the Beam Direct Runner via SQL pipeline
    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size());
  }

  @Test
  public void testMlBucketizerTranslationAndExecution() {
    org.apache.spark.connect.proto.LocalRelation localRel =
        org.apache.spark.connect.proto.LocalRelation.newBuilder()
            .setSchema("id INT, feature DOUBLE")
            .build();
    Relation inputRelation = Relation.newBuilder().setLocalRelation(localRel).build();

    java.util.List<org.apache.spark.connect.proto.Expression.Literal> splitsList =
        java.util.Arrays.asList(
            org.apache.spark.connect.proto.Expression.Literal.newBuilder().setDouble(0.0).build(),
            org.apache.spark.connect.proto.Expression.Literal.newBuilder().setDouble(5.0).build(),
            org.apache.spark.connect.proto.Expression.Literal.newBuilder().setDouble(10.0).build());

    org.apache.spark.connect.proto.Expression.Literal splitsLiteral =
        org.apache.spark.connect.proto.Expression.Literal.newBuilder()
            .setArray(
                org.apache.spark.connect.proto.Expression.Literal.Array.newBuilder()
                    .addAllElements(splitsList)
                    .build())
            .build();

    org.apache.spark.connect.proto.MlParams params =
        org.apache.spark.connect.proto.MlParams.newBuilder()
            .putParams(
                "inputCol",
                org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                    .setString("feature")
                    .build())
            .putParams(
                "outputCol",
                org.apache.spark.connect.proto.Expression.Literal.newBuilder()
                    .setString("bucketized")
                    .build())
            .putParams("splits", splitsLiteral)
            .build();

    MlRelation.Transform transform =
        MlRelation.Transform.newBuilder()
            .setTransformer(MlOperator.newBuilder().setName("Bucketizer").build())
            .setInput(inputRelation)
            .setParams(params)
            .build();

    Any any = Any.pack(transform);
    Relation relation = Relation.newBuilder().setExtension(any).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);
    assertTrue(
        relNode
            instanceof
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject);

    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject
        projectNode =
            (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject)
                relNode;
    assertNotNull(projectNode.getRowType());
    assertEquals(3, projectNode.getRowType().getFieldCount());
    assertEquals("id", projectNode.getRowType().getFieldNames().get(0));
    assertEquals("feature", projectNode.getRowType().getFieldNames().get(1));
    assertEquals("bucketized", projectNode.getRowType().getFieldNames().get(2));

    // Verify execution compiles and executes successfully on Beam Direct Runner
    List<Row> rows = executeRelNode(relNode);
    assertEquals(0, rows.size());
  }

  @Test
  public void testMlRelationTransformUnpacking() {
    org.apache.spark.connect.proto.LocalRelation localRel =
        org.apache.spark.connect.proto.LocalRelation.newBuilder()
            .setSchema("id INT, name STRING")
            .build();
    Relation inputRelation = Relation.newBuilder().setLocalRelation(localRel).build();

    MlRelation.Transform transform =
        MlRelation.Transform.newBuilder()
            .setTransformer(MlOperator.newBuilder().setName("VectorAssembler").build())
            .setInput(inputRelation)
            .build();

    Any any = Any.pack(transform);
    Relation relation = Relation.newBuilder().setExtension(any).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);
    assertTrue(relNode instanceof BeamMlFeature);

    BeamMlFeature featureRel = (BeamMlFeature) relNode;
    assertEquals("VectorAssembler", featureRel.getTransformerName());
    assertNotNull(featureRel.getRowType());
    assertEquals(2, featureRel.getRowType().getFieldCount());
    assertEquals("id", featureRel.getRowType().getFieldNames().get(0));
    assertThrows(UnsupportedOperationException.class, () -> featureRel.buildPTransform());
  }

  @Test
  public void testMlRelationTransformWithObjRefRegistryLookup() {
    String objRefId =
        SparkMLObjectRegistry.getGlobalRegistry()
            .register("LogisticRegressionModel", "lr_54321", "/tmp/my_lr_model");

    org.apache.spark.connect.proto.LocalRelation localRel =
        org.apache.spark.connect.proto.LocalRelation.newBuilder()
            .setSchema("id INT, label DOUBLE")
            .build();
    Relation inputRelation = Relation.newBuilder().setLocalRelation(localRel).build();

    MlRelation.Transform transform =
        MlRelation.Transform.newBuilder()
            .setObjRef(ObjectRef.newBuilder().setId(objRefId).build())
            .setInput(inputRelation)
            .build();

    Any any = Any.pack(transform);
    Relation relation = Relation.newBuilder().setExtension(any).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);
    assertTrue(relNode instanceof BeamMlPredict);

    BeamMlPredict predictRel = (BeamMlPredict) relNode;
    assertNotNull(predictRel.getModelState());
    assertEquals("LogisticRegressionModel", predictRel.getModelState().getOperatorName());
    assertEquals("lr_54321", predictRel.getModelState().getUid());
    assertEquals("/tmp/my_lr_model", predictRel.getModelState().getPath());
    assertNotNull(predictRel.getRowType());
    assertEquals(2, predictRel.getRowType().getFieldCount());
    assertEquals("id", predictRel.getRowType().getFieldNames().get(0));
    assertThrows(UnsupportedOperationException.class, () -> predictRel.buildPTransform());
  }

  @Test
  public void testMlRelationFetchUnpacking() {
    Fetch fetch =
        Fetch.newBuilder()
            .setObjRef(ObjectRef.newBuilder().setId("model_123").build())
            .addMethods(Fetch.Method.newBuilder().setMethod("summary").build())
            .build();

    Any any = Any.pack(fetch);
    Relation relation = Relation.newBuilder().setExtension(any).build();

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> translator.translate(relation));

    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception.getMessage().contains("Spark Connect MLlib Fetch relation is not supported yet"));
    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception.getMessage().contains("Methods count: 1"));
  }

  @Test
  public void testMlWriteUnpacking() {
    MlCommand.Write write =
        MlCommand.Write.newBuilder()
            .setOperator(MlOperator.newBuilder().setName("LogisticRegression").build())
            .setPath("/tmp/lr_model")
            .build();

    Any any = Any.pack(write);
    Relation relation = Relation.newBuilder().setExtension(any).build();

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> translator.translate(relation));

    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception
            .getMessage()
            .contains("Spark Connect MLlib Write extension is not supported yet"));
    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception.getMessage().contains("/tmp/lr_model"));
  }

  @Test
  public void testMlReadUnpacking() {
    MlCommand.Read read =
        MlCommand.Read.newBuilder()
            .setOperator(
                MlOperator.newBuilder().setName("LogisticRegression").setUid("lr_12345").build())
            .setPath("/tmp/lr_model")
            .build();

    Any any = Any.pack(read);
    Relation relation = Relation.newBuilder().setExtension(any).build();

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> translator.translate(relation));

    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception.getMessage().contains("Read extension registered successfully"));
    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception.getMessage().contains("Registered ID: model_"));
    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception.getMessage().contains("LogisticRegression"));
    assertTrue(
        "Actual message: " + exception.getMessage(), exception.getMessage().contains("lr_12345"));
    assertTrue(
        "Actual message: " + exception.getMessage(),
        exception.getMessage().contains("/tmp/lr_model"));
  }

  @Test
  public void testMlGenericRelationUnpacking() {
    org.apache.spark.connect.proto.LocalRelation localRel =
        org.apache.spark.connect.proto.LocalRelation.newBuilder()
            .setSchema("id INT, name STRING")
            .build();
    Relation inputRelation = Relation.newBuilder().setLocalRelation(localRel).build();

    MlRelation.Transform transform =
        MlRelation.Transform.newBuilder()
            .setTransformer(MlOperator.newBuilder().setName("StandardScaler").build())
            .setInput(inputRelation)
            .build();

    MlRelation mlRelation = MlRelation.newBuilder().setTransform(transform).build();

    Any any = Any.pack(mlRelation);
    Relation relation = Relation.newBuilder().setExtension(any).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);
    assertTrue(relNode instanceof BeamMlFeature);

    BeamMlFeature featureRel = (BeamMlFeature) relNode;
    assertEquals("StandardScaler", featureRel.getTransformerName());
    assertNotNull(featureRel.getRowType());
    assertEquals(2, featureRel.getRowType().getFieldCount());
    assertEquals("id", featureRel.getRowType().getFieldNames().get(0));
    assertThrows(UnsupportedOperationException.class, () -> featureRel.buildPTransform());
  }
}
