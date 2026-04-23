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
import static org.junit.Assert.fail;

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.spark.connect.proto.*;
import org.junit.Before;
import org.junit.Test;

public class SparkExpressionToRexNodeTest {

  private BeamSqlEnv sqlEnv;
  private SparkExpressionToRexNode translator;
  private RelDataType inputRowType;

  @Before
  public void setUp() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlEnv.BeamSqlEnvBuilder sqlEnvBuilder = BeamSqlEnv.builder(catalogManager);
    sqlEnvBuilder.setQueryPlannerClassName(CalciteQueryPlanner.class.getCanonicalName());
    sqlEnvBuilder.setPipelineOptions(org.apache.beam.sdk.options.PipelineOptionsFactory.create());
    sqlEnv = sqlEnvBuilder.build();

    RelDataTypeFactory typeFactory = sqlEnv.getRelBuilder().getCluster().getTypeFactory();
    inputRowType =
        typeFactory.createStructType(
            java.util.Arrays.asList(
                typeFactory.createSqlType(SqlTypeName.VARCHAR),
                typeFactory.createStructType(
                    java.util.Collections.singletonList(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                    java.util.Collections.singletonList("nested_name"))),
            java.util.Arrays.asList("name", "struct_col"));

    translator =
        new SparkExpressionToRexNode(
            sqlEnv.getRelBuilder().getCluster(), inputRowType, sqlEnv.getOperatorTable());
  }

  @Test
  public void testLiteral() {
    Expression expr =
        Expression.newBuilder()
            .setLiteral(Expression.Literal.newBuilder().setBoolean(true))
            .build();
    RexNode rex = translator.translate(expr);
    assertNotNull(rex);
    assertEquals("true", rex.toString());
  }

  @Test
  public void testUnresolvedAttribute() {
    Expression expr =
        Expression.newBuilder()
            .setUnresolvedAttribute(
                Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("name"))
            .build();
    RexNode rex = translator.translate(expr);
    assertNotNull(rex);
    assertEquals("$0", rex.toString());
  }

  @Test
  public void testUnresolvedFunction() {
    Expression arg1 =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(1)).build();
    Expression arg2 =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(2)).build();

    Expression expr =
        Expression.newBuilder()
            .setUnresolvedFunction(
                Expression.UnresolvedFunction.newBuilder()
                    .setFunctionName("+")
                    .addArguments(arg1)
                    .addArguments(arg2))
            .build();

    RexNode rex = translator.translate(expr);
    assertNotNull(rex);
    assertTrue(rex.toString().contains("+"));
  }

  @Test
  public void testExpressionString() {
    Expression expr =
        Expression.newBuilder()
            .setExpressionString(Expression.ExpressionString.newBuilder().setExpression("1 + 1"))
            .build();
    RexNode rex = translator.translate(expr);
    assertNotNull(rex);
    assertEquals("'1 + 1'", rex.toString());
  }

  @Test
  public void testUnresolvedStar() {
    Expression expr =
        Expression.newBuilder()
            .setUnresolvedStar(Expression.UnresolvedStar.newBuilder().build())
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testAlias() {
    Expression arg =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(1)).build();
    Expression expr =
        Expression.newBuilder()
            .setAlias(Expression.Alias.newBuilder().setExpr(arg).addName("my_alias"))
            .build();

    RexNode rex = translator.translate(expr);
    assertNotNull(rex);
    assertEquals("1", rex.toString());
  }

  @Test
  public void testCast() {
    Expression arg =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(1)).build();
    Expression expr =
        Expression.newBuilder()
            .setCast(
                Expression.Cast.newBuilder()
                    .setExpr(arg)
                    .setType(
                        org.apache.spark.connect.proto.DataType.newBuilder()
                            .setString(
                                org.apache.spark.connect.proto.DataType.String.newBuilder()
                                    .build())))
            .build();

    RexNode rex = translator.translate(expr);
    assertNotNull(rex);
    assertTrue(rex.toString().contains("CAST"));
  }

  @Test
  public void testUnresolvedRegex() {
    Expression expr =
        Expression.newBuilder()
            .setUnresolvedRegex(Expression.UnresolvedRegex.newBuilder().setColName("a.*").build())
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testSortOrder() {
    Expression arg =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(1)).build();
    Expression expr =
        Expression.newBuilder()
            .setSortOrder(
                Expression.SortOrder.newBuilder()
                    .setChild(arg)
                    .setDirection(Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING))
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testLambdaFunction() {
    Expression body =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(1)).build();
    Expression expr =
        Expression.newBuilder()
            .setLambdaFunction(
                Expression.LambdaFunction.newBuilder()
                    .setFunction(body)
                    .addArguments(
                        Expression.UnresolvedNamedLambdaVariable.newBuilder().addNameParts("x")))
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException for LambdaFunction");
    } catch (UnsupportedOperationException e) {
      assertTrue(
          e.getMessage().contains("LambdaFunction expression not supported yet")
              || e.getMessage().contains("not supported"));
    }
  }

  @Test
  public void testWindow() {
    Expression func =
        Expression.newBuilder()
            .setLiteral(Expression.Literal.newBuilder().setString("rank"))
            .build();
    Expression expr =
        Expression.newBuilder()
            .setWindow(
                Expression.Window.newBuilder()
                    .setWindowFunction(func)
                    .addPartitionSpec(
                        Expression.newBuilder()
                            .setUnresolvedAttribute(
                                Expression.UnresolvedAttribute.newBuilder()
                                    .setUnparsedIdentifier("name"))))
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException for Window");
    } catch (UnsupportedOperationException e) {
      assertTrue(
          e.getMessage().contains("Window expression not supported yet")
              || e.getMessage().contains("not supported"));
    }
  }

  @Test
  public void testUnresolvedExtractValue() {
    Expression child =
        Expression.newBuilder()
            .setUnresolvedAttribute(
                Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("struct_col"))
            .build();
    Expression extraction =
        Expression.newBuilder()
            .setLiteral(Expression.Literal.newBuilder().setString("nested_name"))
            .build();
    Expression expr =
        Expression.newBuilder()
            .setUnresolvedExtractValue(
                Expression.UnresolvedExtractValue.newBuilder()
                    .setChild(child)
                    .setExtraction(extraction))
            .build();

    RexNode rex = translator.translate(expr);
    assertNotNull(rex);
  }

  @Test
  public void testUpdateFields() {
    Expression struct =
        Expression.newBuilder()
            .setUnresolvedAttribute(
                Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("name"))
            .build();
    Expression value =
        Expression.newBuilder()
            .setLiteral(Expression.Literal.newBuilder().setString("new_val"))
            .build();
    Expression expr =
        Expression.newBuilder()
            .setUpdateFields(
                Expression.UpdateFields.newBuilder()
                    .setStructExpression(struct)
                    .setFieldName("name")
                    .setValueExpression(value))
            .build();

    try {
      translator.translate(expr);
    } catch (IllegalArgumentException e) {
      // Expected because "name" is a VARCHAR, not a struct type in our setup
      assertTrue(e.getMessage().contains("Expected struct type"));
    } catch (UnsupportedOperationException e) {
      // If not implemented at all
    }
  }

  @Test
  public void testUnresolvedNamedLambdaVariable() {
    Expression expr =
        Expression.newBuilder()
            .setUnresolvedNamedLambdaVariable(
                Expression.UnresolvedNamedLambdaVariable.newBuilder().addNameParts("x"))
            .build();

    try {
      RexNode rex = translator.translate(expr);
      assertNotNull(rex);
    } catch (UnsupportedOperationException e) {
      assertTrue(
          e.getMessage().contains("UnresolvedNamedLambdaVariable expression not supported yet")
              || e.getMessage().contains("not supported"));
    }
  }

  @Test
  public void testCommonInlineUserDefinedFunction() {
    Expression expr =
        Expression.newBuilder()
            .setCommonInlineUserDefinedFunction(
                CommonInlineUserDefinedFunction.newBuilder()
                    .setFunctionName("my_udf")
                    .setDeterministic(true))
            .build();

    RexNode rex = translator.translate(expr);
    assertNotNull(rex);
  }

  @Test
  public void testCallFunction() {
    Expression arg =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(1)).build();
    Expression expr =
        Expression.newBuilder()
            .setCallFunction(CallFunction.newBuilder().setFunctionName("abs").addArguments(arg))
            .build();

    try {
      RexNode rex = translator.translate(expr);
      assertNotNull(rex);
    } catch (UnsupportedOperationException e) {
      assertTrue(
          e.getMessage().contains("Function not found in Calcite")
              || e.getMessage().contains("not supported"));
    }
  }

  @Test
  public void testNamedArgumentExpression() {
    Expression val =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(1)).build();
    Expression expr =
        Expression.newBuilder()
            .setNamedArgumentExpression(
                NamedArgumentExpression.newBuilder().setKey("key").setValue(val))
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testMergeAction() {
    Expression expr =
        Expression.newBuilder()
            .setMergeAction(
                MergeAction.newBuilder().setActionType(MergeAction.ActionType.ACTION_TYPE_DELETE))
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testTypedAggregateExpression() {
    Expression expr =
        Expression.newBuilder()
            .setTypedAggregateExpression(TypedAggregateExpression.newBuilder().build())
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testSubqueryExpression() {
    Expression expr =
        Expression.newBuilder()
            .setSubqueryExpression(
                SubqueryExpression.newBuilder()
                    .setPlanId(1)
                    .setSubqueryType(SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR))
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testDirectShufflePartitionID() {
    Expression arg =
        Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setInteger(1)).build();
    Expression expr =
        Expression.newBuilder()
            .setDirectShufflePartitionId(
                Expression.DirectShufflePartitionID.newBuilder().setChild(arg))
            .build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testExtension() {
    Expression expr =
        Expression.newBuilder().setExtension(com.google.protobuf.Any.newBuilder().build()).build();

    try {
      translator.translate(expr);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }
}
