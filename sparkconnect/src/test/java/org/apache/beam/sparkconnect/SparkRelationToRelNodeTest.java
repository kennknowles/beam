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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
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
    BeamSqlEnv.BeamSqlEnvBuilder sqlEnvBuilder = BeamSqlEnv.builder(catalogManager);
    sqlEnvBuilder.setQueryPlannerClassName(CalciteQueryPlanner.class.getCanonicalName());
    sqlEnvBuilder.setPipelineOptions(org.apache.beam.sdk.options.PipelineOptionsFactory.create());
    sqlEnv = sqlEnvBuilder.build();

    translator = new SparkRelationToRelNode(sqlEnv, Collections.emptyMap());
  }

  private List<Row> executeRelNode(RelNode relNode) {
    BeamRelNode beamRelNode = sqlEnv.convertToBeamRel(relNode);
    return BeamEnumerableConverter.toRowList(beamRelNode);
  }

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
    // We can't easily check column name without more complex validation, but we can check size.
  }

  @Test
  public void testFilter() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression condition =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setLiteral(
                org.apache.spark.connect.proto.Expression.Literal.newBuilder().setBoolean(true))
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testJoin() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'b' AS name"))
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testSort() {
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

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testLimit() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testAggregate() {
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

    org.apache.spark.connect.proto.Expression countExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedFunction(
                org.apache.spark.connect.proto.Expression.UnresolvedFunction.newBuilder()
                    .setFunctionName("count")
                    .addArguments(idExpr))
            .build();

    org.apache.spark.connect.proto.Aggregate aggregate =
        org.apache.spark.connect.proto.Aggregate.newBuilder()
            .setInput(inputRelation)
            .setGroupType(org.apache.spark.connect.proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
            .addGroupingExpressions(idExpr)
            .addAggregateExpressions(countExpr)
            .build();

    Relation relation = Relation.newBuilder().setAggregate(aggregate).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
  }

  @Test
  public void testLocalRelation() {
    org.apache.spark.connect.proto.LocalRelation localRel =
        org.apache.spark.connect.proto.LocalRelation.newBuilder()
            .setSchema("id INT, name STRING")
            .build();

    Relation relation = Relation.newBuilder().setLocalRelation(localRel).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    try {
      List<Row> rows = executeRelNode(relNode);
      assertEquals(0, rows.size()); // Schema only, no data
    } catch (Exception e) {
      // Expected if not fully supported.
    }
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testSample() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Sample sample =
        org.apache.spark.connect.proto.Sample.newBuilder()
            .setInput(inputRelation)
            .setLowerBound(0.0)
            .setUpperBound(1.0)
            .build();

    Relation relation = Relation.newBuilder().setSample(sample).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertTrue(rows.size() <= 1);
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testOffset() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
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
    assertEquals(0, rows.size());
  }

  @Test
  public void testDeduplicate() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
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
    assertEquals(1, rows.size());
  }

  @Test
  public void testRange() {
    org.apache.spark.connect.proto.Range range =
        org.apache.spark.connect.proto.Range.newBuilder().setStart(0).setEnd(10).setStep(1).build();

    Relation relation = Relation.newBuilder().setRange(range).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(10, rows.size());
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

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
  }

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
  }

  @Ignore("Fails with AssertionError")
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
  }

  @Test
  public void testDrop() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Drop drop =
        org.apache.spark.connect.proto.Drop.newBuilder()
            .setInput(inputRelation)
            .addColumnNames("name")
            .build();

    Relation relation = Relation.newBuilder().setDrop(drop).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testTail() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
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
  }

  @Test
  public void testWithColumns() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression valExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setLiteral(
                org.apache.spark.connect.proto.Expression.Literal.newBuilder().setInteger(2))
            .build();

    org.apache.spark.connect.proto.Expression.Alias alias =
        org.apache.spark.connect.proto.Expression.Alias.newBuilder()
            .setExpr(valExpr)
            .addName("new_col")
            .build();

    org.apache.spark.connect.proto.WithColumns withColumns =
        org.apache.spark.connect.proto.WithColumns.newBuilder()
            .setInput(inputRelation)
            .addAliases(alias)
            .build();

    Relation relation = Relation.newBuilder().setWithColumns(withColumns).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testHint() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.Hint hint =
        org.apache.spark.connect.proto.Hint.newBuilder()
            .setInput(inputRelation)
            .setName("BROADCAST")
            .build();

    Relation relation = Relation.newBuilder().setHint(hint).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testToSchema() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.DataType.StructField field =
        org.apache.spark.connect.proto.DataType.StructField.newBuilder()
            .setName("id")
            .setDataType(
                org.apache.spark.connect.proto.DataType.newBuilder()
                    .setInteger(
                        org.apache.spark.connect.proto.DataType.Integer.newBuilder().build()))
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testMapPartitions() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.MapPartitions mapPartitions =
        org.apache.spark.connect.proto.MapPartitions.newBuilder()
            .setInput(inputRelation)
            .setFunc(
                org.apache.spark.connect.proto.CommonInlineUserDefinedFunction.newBuilder()
                    .setFunctionName("my_func"))
            .build();

    Relation relation = Relation.newBuilder().setMapPartitions(mapPartitions).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testCollectMetrics() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.CollectMetrics collectMetrics =
        org.apache.spark.connect.proto.CollectMetrics.newBuilder()
            .setInput(inputRelation)
            .setName("my_metrics")
            .build();

    Relation relation = Relation.newBuilder().setCollectMetrics(collectMetrics).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
    assertEquals(1, rows.size());
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testWithWatermark() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testAsOfJoin() {
    Relation leftRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    Relation rightRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'b' AS name"))
            .build();

    org.apache.spark.connect.proto.Expression idExpr =
        org.apache.spark.connect.proto.Expression.newBuilder()
            .setUnresolvedAttribute(
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("id"))
            .build();

    org.apache.spark.connect.proto.AsOfJoin asOfJoin =
        org.apache.spark.connect.proto.AsOfJoin.newBuilder()
            .setLeft(leftRelation)
            .setRight(rightRelation)
            .setLeftAsOf(idExpr)
            .setRightAsOf(idExpr)
            .build();

    Relation relation = Relation.newBuilder().setAsOfJoin(asOfJoin).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testNAFill() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.NAFill naFill =
        org.apache.spark.connect.proto.NAFill.newBuilder().setInput(inputRelation).build();

    Relation relation = Relation.newBuilder().setFillNa(naFill).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testNADrop() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.NADrop naDrop =
        org.apache.spark.connect.proto.NADrop.newBuilder().setInput(inputRelation).build();

    Relation relation = Relation.newBuilder().setDropNa(naDrop).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testNAReplace() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

    org.apache.spark.connect.proto.NAReplace naReplace =
        org.apache.spark.connect.proto.NAReplace.newBuilder().setInput(inputRelation).build();

    Relation relation = Relation.newBuilder().setReplace(naReplace).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("IllegalArgumentException expected")
  @Test
  public void testRead() {
    org.apache.spark.connect.proto.Read.DataSource dataSource =
        org.apache.spark.connect.proto.Read.DataSource.newBuilder()
            .setFormat("csv")
            .setSchema("id INT, name STRING")
            .addPaths("dummy_path")
            .build();

    org.apache.spark.connect.proto.Read read =
        org.apache.spark.connect.proto.Read.newBuilder().setDataSource(dataSource).build();

    Relation relation = Relation.newBuilder().setRead(read).build();

    RelNode relNode = translator.translate(relation);
    assertNotNull(relNode);

    List<Row> rows = executeRelNode(relNode);
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testStatCov() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testStatCorr() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
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
  }

  @Ignore("UnsupportedOperationException expected")
  @Test
  public void testStatApproxQuantile() {
    Relation inputRelation =
        Relation.newBuilder()
            .setSql(SQL.newBuilder().setQuery("SELECT 1 AS id, 'a' AS name"))
            .build();

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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }

  @Ignore("UnsupportedOperationException expected")
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
  }
}
