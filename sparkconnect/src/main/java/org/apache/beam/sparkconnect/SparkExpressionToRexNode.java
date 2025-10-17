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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSyntax;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.DateString;
import org.apache.spark.connect.proto.CallFunction;
import org.apache.spark.connect.proto.Expression;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SparkExpressionToRexNode {
  private final RelDataType inputRowType;
  private final SparkDataTypeToRelDataType typeConverter;
  private final RelOptCluster cluster;

  public SparkExpressionToRexNode(RelOptCluster cluster, RelDataType inputRowType) {
    this.cluster = cluster;
    this.inputRowType = inputRowType;
    this.typeConverter = new SparkDataTypeToRelDataType(cluster.getTypeFactory());
  }

  public RexNode translate(Expression expr) {
    switch (expr.getExprTypeCase()) {
      case LITERAL:
        return translateLiteral(expr.getLiteral());
      case UNRESOLVED_ATTRIBUTE:
        return translateUnresolvedAttribute(expr.getUnresolvedAttribute());
      case CALL_FUNCTION:
        return translateCallFunction(expr.getCallFunction());
      case CAST: // *** ADDED CASE ***
        return translateCast(expr.getCast());
      default:
        throw new UnsupportedOperationException(
            "Spark Expression type not supported: " + expr.getExprTypeCase());
    }
  }

  // --- Conversion Methods for Expression Types ---

  private RelDataType getLiteralType(Expression.Literal literal) {
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    if (literal.hasDataType()) {
      // If DataType is provided, use the dedicated converter.
      return typeConverter.sparkDataTypeToRelDataType(literal.getDataType());
    }

    // Infer from the literal value type if DataType is not provided
    switch (literal.getLiteralTypeCase()) {
      case NULL:
        // According to the proto, DataType is required for NULLs.
        throw new IllegalArgumentException(
            "NULL literals must have the data_type field set to determine their type.");
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case BYTE:
        return typeFactory.createSqlType(SqlTypeName.TINYINT);
      case SHORT:
        return typeFactory.createSqlType(SqlTypeName.SMALLINT);
      case INTEGER:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.FLOAT);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case DECIMAL:
        // Fallback to precision/scale in the literal if DataType is missing.
        int precision =
            literal.getDecimal().hasPrecision()
                ? literal.getDecimal().getPrecision()
                : 38; // Spark's default max precision
        int scale = literal.getDecimal().hasScale() ? literal.getDecimal().getScale() : 18;
        return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
      case STRING:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case BINARY:
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      case DATE:
        return typeFactory.createSqlType(SqlTypeName.DATE);
      case TIMESTAMP:
        // Corresponds to Spark's TimestampType (with session time zone)
        // Calcite's TIMESTAMP has no time zone, which is the base type.
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      case TIMESTAMP_NTZ:
        // Corresponds to Spark's TimestampNTZType (local time zone)
        // Calcite's TIMESTAMP is the closest representation.
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      case TIME:
        return typeFactory.createSqlType(SqlTypeName.TIME);

      case ARRAY:
      case MAP:
      case STRUCT:
        throw new IllegalArgumentException(
            "Complex literals (ARRAY, MAP, STRUCT) must have the data_type field set.");

      case CALENDAR_INTERVAL:
        // This is a complex type in Spark. Approximating with a Calcite interval.
        // The DataType field should ideally be present for precise mapping.
        return typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_SECOND);
      case YEAR_MONTH_INTERVAL:
        return typeFactory.createSqlType(SqlTypeName.INTERVAL_YEAR_MONTH);
      case DAY_TIME_INTERVAL:
        return typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_SECOND);

      default:
        throw new UnsupportedOperationException(
            "Literal type not supported for type inference: " + literal.getLiteralTypeCase());
    }
  }

  private RexNode translateLiteral(Expression.Literal literal) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    switch (literal.getLiteralTypeCase()) {
      case NULL:
        return rexBuilder.makeNullLiteral(
            typeConverter.sparkDataTypeToRelDataType(literal.getNull()));
      case BOOLEAN:
        return rexBuilder.makeLiteral(literal.getBoolean());
      case STRING:
        return rexBuilder.makeLiteral(literal.getString());
      case BYTE:
        return rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(literal.getByte()), typeFactory.createSqlType(SqlTypeName.TINYINT));
      case SHORT:
        return rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(literal.getShort()),
            typeFactory.createSqlType(SqlTypeName.SMALLINT));
      case INTEGER:
        return rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(literal.getInteger()),
            typeFactory.createSqlType(SqlTypeName.INTEGER));
      case LONG:
        return rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(literal.getLong()), typeFactory.createSqlType(SqlTypeName.BIGINT));
      case FLOAT:
        return rexBuilder.makeApproxLiteral(
            BigDecimal.valueOf(literal.getFloat()), typeFactory.createSqlType(SqlTypeName.FLOAT));
      case DOUBLE:
        return rexBuilder.makeApproxLiteral(
            BigDecimal.valueOf(literal.getDouble()), typeFactory.createSqlType(SqlTypeName.DOUBLE));
      case DECIMAL:
        return rexBuilder.makeExactLiteral(
            new BigDecimal(literal.getDecimal().getValue()),
            typeConverter.sparkDataTypeToRelDataType(
                literal.getDataType())); // DECIMAL type requires precision/scale
      case DATE:
        // Calcite DateLiteral from days since epoch
        return rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch(literal.getDate()));
        //      case TIMESTAMP:
        //        // Spark Timestamp is micros since epoch. Calcite is millis.
        //        long millis = literal.getTimestamp() / 1000;
        //        return
        // rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis),
        // SqlTypeName.TIMESTAMP.getDefaultPrecision());
        // TODO: Handle other literal types: BINARY, ARRAY, MAP, STRUCT, INTERVALS
      default:
        throw new UnsupportedOperationException(
            "Literal type not supported: " + literal.getLiteralTypeCase());
    }
  }

  private RexNode translateUnresolvedAttribute(Expression.UnresolvedAttribute attr) {
    String name = attr.getUnparsedIdentifier();
    // Note: Calcite's getField is case-insensitive by default. Spark SQL is case-insensitive
    // by default.
    RelDataTypeField field = inputRowType.getField(name, false, false);
    if (field == null) {
      throw new IllegalArgumentException(
          "Column not found: " + name + " in input schema: " + inputRowType);
    }
    return cluster.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
  }

  private RexNode translateCast(Expression.Cast cast) {
    RexNode operand = translate(cast.getExpr());
    RelDataType targetType;
    if (cast.hasType()) {
      targetType = typeConverter.sparkDataTypeToRelDataType(cast.getType());
    } else {
      // TODO: Support parsing type_str if necessary, though this is server-side
      throw new UnsupportedOperationException(
          "Casting to type_str is not supported in this translator.");
    }
    return cluster.getRexBuilder().makeCast(targetType, operand);
  }

  private RexNode translateCallFunction(CallFunction func) {
    String funcName = func.getFunctionName();
    List<RexNode> operands =
        func.getArgumentsList().stream().map(this::translate).collect(Collectors.toList());

    // Lookup the operator in Calcite's tables
    // TODO: Enhance operator lookup (case-insensitivity, multiple tables)
    List<SqlOperator> operators = new java.util.ArrayList<>();
    SqlStdOperatorTable.instance()
        .lookupOperatorOverloads(
            new SqlIdentifier(funcName, SqlParserPos.ZERO),
            null,
            SqlSyntax.FUNCTION,
            operators,
            SqlNameMatchers.liberal());

    if (operators.isEmpty()) {
      throw new UnsupportedOperationException("Function not found in Calcite: " + funcName);
    }

    // TODO: Add more sophisticated overload resolution based on operand types.
    // For now, picking the first one.
    SqlOperator operator = operators.get(0);

    return cluster.getRexBuilder().makeCall(operator, operands);
  }

  /**
   * Derives the output field names for a list of Spark Connect Expressions to be used in a Calcite
   * LogicalProject.
   *
   * @param sparkExpressions The list of expressions from the Project proto.
   * @return A list of strings, where each element is the desired field name or null if the name
   *     should be derived by Calcite.
   */
  public List<@Nullable String> deriveFieldNames(List<Expression> sparkExpressions) {
    return sparkExpressions.stream()
        .<@Nullable String>map(this::deriveFieldName)
        .collect(Collectors.toList());
  }

  private @Nullable String deriveFieldName(Expression sparkExpression) {
    switch (sparkExpression.getExprTypeCase()) {
      case ALIAS:
        // TODO: multi-part aliases occur but not in column alias context
        checkArgument(
            sparkExpression.getAlias().getNameCount() == 0,
            "Multi-part alias cannot occur in expression context");
        return sparkExpression.getAlias().getName(0);
      case UNRESOLVED_ATTRIBUTE:
        // For an unparsed attribute like `table.col` the name will be `col`
        String unparsed = sparkExpression.getUnresolvedAttribute().getUnparsedIdentifier();
        int dotIndex = unparsed.lastIndexOf('.');
        return (dotIndex == -1) ? unparsed : unparsed.substring(dotIndex + 1);

      default:
        // For any other expression type not explicitly aliased,
        // pass null to let Calcite derive the field name (e.g., $f0, EXPR$1, or based on function
        // name).
        return null;
    }
  }
}
