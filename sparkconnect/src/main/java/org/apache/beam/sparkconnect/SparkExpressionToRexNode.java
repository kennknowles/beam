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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSyntax;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.DateString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.spark.connect.proto.CallFunction;
import org.apache.spark.connect.proto.Expression;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkExpressionToRexNode {
  private static final Logger LOG = LoggerFactory.getLogger(SparkExpressionToRexNode.class);

  private final RelDataType inputRowType;
  private final SparkDataTypeToRelDataType typeConverter;
  private final RelOptCluster cluster;
  private final SqlOperatorTable operatorTable;

  public SparkExpressionToRexNode(
      RelOptCluster cluster, RelDataType inputRowType, SqlOperatorTable operatorTable) {
    this.cluster = cluster;
    this.inputRowType = inputRowType;
    this.operatorTable = operatorTable;
    this.typeConverter = new SparkDataTypeToRelDataType(cluster.getTypeFactory());
  }

  // A map for common Spark function names to Calcite operators.
  private static final Map<String, SqlOperator> OPERATOR_MAP =
      ImmutableMap.<String, SqlOperator>builder()
          .put("==", SqlStdOperatorTable.EQUALS)
          .put("=", SqlStdOperatorTable.EQUALS)
          .put("!=", SqlStdOperatorTable.NOT_EQUALS)
          .put("<>", SqlStdOperatorTable.NOT_EQUALS)
          .put(">", SqlStdOperatorTable.GREATER_THAN)
          .put("<", SqlStdOperatorTable.LESS_THAN)
          .put(">=", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)
          .put("<=", SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
          .put("and", SqlStdOperatorTable.AND)
          .put("or", SqlStdOperatorTable.OR)
          .put("not", SqlStdOperatorTable.NOT)
          .put("+", SqlStdOperatorTable.PLUS)
          .put("-", SqlStdOperatorTable.MINUS)
          .put("*", SqlStdOperatorTable.MULTIPLY)
          .put("/", SqlStdOperatorTable.DIVIDE)
          .put("negative", SqlStdOperatorTable.UNARY_MINUS)
          .put("isNull", SqlStdOperatorTable.IS_NULL)
          .put("isNotNull", SqlStdOperatorTable.IS_NOT_NULL)
          .put("in", SqlStdOperatorTable.IN)
          .put("%", SqlStdOperatorTable.MOD)
          .put("&", SqlStdOperatorTable.BITAND)
          .put("bitwiseAND", SqlStdOperatorTable.BITAND)
          .put("|", SqlStdOperatorTable.BITOR)
          .put("bitwiseOR", SqlStdOperatorTable.BITOR)
          .put("^", SqlStdOperatorTable.BITXOR)
          .put("bitwiseXOR", SqlStdOperatorTable.BITXOR)
          .put("between", SqlStdOperatorTable.BETWEEN)
          .build();

  public RexNode translate(Expression expr) {
    switch (expr.getExprTypeCase()) {
      case LITERAL:
        return translateLiteral(expr.getLiteral());
      case UNRESOLVED_ATTRIBUTE:
        return translateUnresolvedAttribute(expr.getUnresolvedAttribute());
      case CALL_FUNCTION:
        return translateUnresolvedFunction(
            Expression.UnresolvedFunction.newBuilder()
                .setFunctionName(expr.getCallFunction().getFunctionName())
                .addAllArguments(expr.getCallFunction().getArgumentsList())
                .build());
      case UNRESOLVED_FUNCTION:
        return translateUnresolvedFunction(expr.getUnresolvedFunction());
      case CAST: // *** ADDED CASE ***
        return translateCast(expr.getCast());
      case EXPRESSION_STRING:
        return translateExpressionString(expr.getExpressionString());
      case ALIAS:
        return translate(expr.getAlias().getExpr());
      default:
        throw new UnsupportedOperationException(
            "Spark Expression type not supported: " + expr.getExprTypeCase());
    }
  }

  // --- Conversion Methods for Expression Types ---

  // --- New method to handle UnresolvedFunction ---
  private RexNode translateUnresolvedFunction(Expression.UnresolvedFunction func) {
    String funcName = func.getFunctionName();
    List<RexNode> operands =
        func.getArgumentsList().stream().map(this::translate).collect(Collectors.toList());
    LOG.info("Translating unresolved function: {} with {} operands.", funcName, operands.size());

    if (funcName.equalsIgnoreCase("in")) {
      if (operands.size() <= 1) {
        return cluster.getRexBuilder().makeLiteral(false);
      }
      RexNode exprNode = operands.get(0);
      RexNode result =
          cluster.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, exprNode, operands.get(1));
      for (int i = 2; i < operands.size(); i++) {
        result =
            cluster
                .getRexBuilder()
                .makeCall(
                    SqlStdOperatorTable.OR,
                    result,
                    cluster
                        .getRexBuilder()
                        .makeCall(SqlStdOperatorTable.EQUALS, exprNode, operands.get(i)));
      }
      return result;
    }

    if (funcName.equalsIgnoreCase("try_mod") && operands.size() == 2) {
      RexNode a = operands.get(0);
      RexNode b = operands.get(1);
      RexBuilder builder = cluster.getRexBuilder();
      // Translate to: CASE WHEN b = 0 THEN NULL ELSE MOD(a, b) END
      RexNode isZero =
          builder.makeCall(
              SqlStdOperatorTable.EQUALS, b, builder.makeExactLiteral(java.math.BigDecimal.ZERO));
      RexNode nullNode = builder.makeNullLiteral(a.getType());
      RexNode modNode = builder.makeCall(SqlStdOperatorTable.MOD, a, b);
      return builder.makeCall(SqlStdOperatorTable.CASE, isZero, nullNode, modNode);
    }

    // Special handling for TRIM, LTRIM, RTRIM which have different signatures in Calcite
    if (funcName.equalsIgnoreCase("trim") && operands.size() == 1) {
      return cluster
          .getRexBuilder()
          .makeCall(
              SqlStdOperatorTable.TRIM,
              cluster
                  .getRexBuilder()
                  .makeLiteral(
                      SqlTrimFunction.Flag.BOTH,
                      cluster.getTypeFactory().createSqlType(SqlTypeName.SYMBOL),
                      false),
              cluster.getRexBuilder().makeLiteral(" "),
              operands.get(0));
    }
    if (funcName.equalsIgnoreCase("ltrim") && operands.size() == 1) {
      return cluster
          .getRexBuilder()
          .makeCall(
              SqlStdOperatorTable.TRIM,
              cluster
                  .getRexBuilder()
                  .makeLiteral(
                      SqlTrimFunction.Flag.LEADING,
                      cluster.getTypeFactory().createSqlType(SqlTypeName.SYMBOL),
                      false),
              cluster.getRexBuilder().makeLiteral(" "),
              operands.get(0));
    }
    if (funcName.equalsIgnoreCase("rtrim") && operands.size() == 1) {
      return cluster
          .getRexBuilder()
          .makeCall(
              SqlStdOperatorTable.TRIM,
              cluster
                  .getRexBuilder()
                  .makeLiteral(
                      SqlTrimFunction.Flag.TRAILING,
                      cluster.getTypeFactory().createSqlType(SqlTypeName.SYMBOL),
                      false),
              cluster.getRexBuilder().makeLiteral(" "),
              operands.get(0));
    }

    // Special handling for startswith/endswith/contains: look them up dynamically
    if (funcName.equalsIgnoreCase("startswith")
        || funcName.equalsIgnoreCase("endswith")
        || funcName.equalsIgnoreCase("contains")) {
      String calciteName =
          funcName.equalsIgnoreCase("startswith")
              ? "STARTS_WITH"
              : funcName.equalsIgnoreCase("endswith") ? "ENDS_WITH" : "CONTAINS";
      List<SqlOperator> ops = new ArrayList<>();
      operatorTable.lookupOperatorOverloads(
          new SqlIdentifier(calciteName, SqlParserPos.ZERO),
          null,
          SqlSyntax.FUNCTION,
          ops,
          SqlNameMatchers.liberal());
      if (!ops.isEmpty()) {
        return cluster.getRexBuilder().makeCall(ops.get(0), operands);
      } else if (funcName.equalsIgnoreCase("contains")) {
        // Fallback for contains if CONTAINS operator is not found: POSITION(substring, string) > 0
        // Operands for Spark contains are [string, substring]
        return cluster
            .getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                cluster
                    .getRexBuilder()
                    .makeCall(SqlStdOperatorTable.POSITION, operands.get(1), operands.get(0)),
                cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO));
      }
    }

    // First, check our map for common operators like '=='
    if (OPERATOR_MAP.containsKey(funcName)) {
      return cluster.getRexBuilder().makeCall(OPERATOR_MAP.get(funcName), operands);
    }

    // If not in the map, use Calcite's operator table lookup
    List<SqlOperator> operators = new ArrayList<>();
    operatorTable.lookupOperatorOverloads(
        new SqlIdentifier(funcName, SqlParserPos.ZERO),
        null,
        SqlSyntax.FUNCTION,
        operators,
        SqlNameMatchers.liberal());

    if (operators.isEmpty()) {
      // Try again with uppercase SNAKE_CASE if it was camelCase (Spark style)
      String snakeCase = funcName.replaceAll("([a-z])([A-Z]+)", "$1_$2").toUpperCase();
      if (!snakeCase.equals(funcName)) {
        operatorTable.lookupOperatorOverloads(
            new SqlIdentifier(snakeCase, SqlParserPos.ZERO),
            null,
            SqlSyntax.FUNCTION,
            operators,
            SqlNameMatchers.liberal());
      }
    }

    if (operators.isEmpty()) {
      if (funcName.equalsIgnoreCase("show_string")) {
        return cluster.getRexBuilder().makeLiteral("show_string");
      }
      throw new UnsupportedOperationException("Function not found in Calcite: " + funcName);
    }

    // TODO: Add more sophisticated overload resolution based on operand types.
    SqlOperator operator = operators.get(0);

    return cluster.getRexBuilder().makeCall(operator, operands);
  }

  private RexNode translateCallFunction(CallFunction func) {
    String funcName = func.getFunctionName();
    List<RexNode> operands =
        func.getArgumentsList().stream().map(this::translate).collect(Collectors.toList());

    // Lookup the operator in the provided operator table
    // TODO: Enhance operator lookup (case-insensitivity, multiple tables)
    List<SqlOperator> operators = new java.util.ArrayList<>();
    LOG.info("Translating unresolved function: {} with {} operands.", funcName, operands.size());
    operatorTable.lookupOperatorOverloads(
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

  private RexNode translateExpressionString(Expression.ExpressionString exprString) {
    // Just return a dummy literal since we don't parse SQL expression strings currently.
    return cluster.getRexBuilder().makeLiteral(exprString.getExpression());
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
        BigDecimal decimalValue = new BigDecimal(literal.getDecimal().getValue());
        int precision = Math.max(1, decimalValue.precision());
        int scale = decimalValue.scale();
        return rexBuilder.makeExactLiteral(
            decimalValue, typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale));
      case DATE:
        // Calcite DateLiteral from days since epoch
        return rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch(literal.getDate()));
      case TIMESTAMP:
        // Spark Timestamp is micros since epoch. Calcite is millis.
        long millis = literal.getTimestamp() / 1000;
        return rexBuilder.makeTimestampLiteral(
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.TimestampString
                .fromMillisSinceEpoch(millis),
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP).getPrecision());
      default:
        throw new UnsupportedOperationException(
            "Literal type not supported: " + literal.getLiteralTypeCase());
    }
  }

  private RexNode translateUnresolvedAttribute(Expression.UnresolvedAttribute attr) {
    String name = attr.getUnparsedIdentifier();
    List<String> parts = Splitter.on('.').splitToList(name);

    RelDataTypeField field = inputRowType.getField(parts.get(0), false, false);
    if (field == null) {
      field = inputRowType.getField(parts.get(0) + "__ntz", false, false);
    }
    if (field == null) {
      throw new IllegalArgumentException(
          "Column not found: " + parts.get(0) + " in input schema: " + inputRowType);
    }

    RexNode node = cluster.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

    for (int i = 1; i < parts.size(); i++) {
      if (node.getType().getSqlTypeName() == SqlTypeName.ARRAY) {
        LOG.warn(
            "Field access on ARRAY is not fully supported. Using hack to extract first element for field: {}",
            parts.get(i));
        // Hack: Extract the first element of the array and access the field on it, then wrap in
        // array
        // Add null check to avoid failing on null arrays
        RexNode isNull = cluster.getRexBuilder().makeCall(SqlStdOperatorTable.IS_NULL, node);
        RexNode firstElement =
            cluster
                .getRexBuilder()
                .makeCall(
                    SqlStdOperatorTable.ITEM,
                    node,
                    cluster.getRexBuilder().makeExactLiteral(BigDecimal.ONE));
        RexNode fieldAccess =
            cluster.getRexBuilder().makeFieldAccess(firstElement, parts.get(i), false);
        RexNode arrayVal =
            cluster
                .getRexBuilder()
                .makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, fieldAccess);

        return cluster
            .getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.CASE,
                isNull,
                cluster.getRexBuilder().makeNullLiteral(arrayVal.getType()),
                arrayVal);
      }
      node = cluster.getRexBuilder().makeFieldAccess(node, parts.get(i), false);
    }

    return node;
  }

  private RexNode translateCast(Expression.Cast cast) {
    RexNode operand = translate(cast.getExpr());
    RelDataType targetType;
    if (cast.hasType()) {
      targetType = typeConverter.sparkDataTypeToRelDataType(cast.getType());
    } else if (cast.hasTypeStr()) {
      String typeStr = cast.getTypeStr().toLowerCase();
      if (typeStr.contains("string")) {
        targetType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
      } else if (typeStr.contains("int")) {
        targetType = cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
      } else if (typeStr.contains("double")
          || typeStr.contains("float")
          || typeStr.contains("real")) {
        targetType = cluster.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
      } else if (typeStr.contains("boolean")) {
        targetType = cluster.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
      } else {
        targetType = cluster.getTypeFactory().createSqlType(SqlTypeName.ANY);
      }
    } else {
      throw new UnsupportedOperationException(
          "Casting to type_str is not supported in this translator.");
    }
    return cluster.getRexBuilder().makeCast(targetType, operand);
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
            sparkExpression.getAlias().getNameCount() <= 1,
            "Multi-part alias cannot occur in expression context");
        return sparkExpression.getAlias().getNameCount() > 0
            ? sparkExpression.getAlias().getName(0)
            : null;
      case UNRESOLVED_ATTRIBUTE:
        // For an unparsed attribute like `table.col` the name will be `col`
        String unparsed = sparkExpression.getUnresolvedAttribute().getUnparsedIdentifier();
        int dotIndex = unparsed.lastIndexOf('.');
        String fieldName = (dotIndex == -1) ? unparsed : unparsed.substring(dotIndex + 1);
        RelDataTypeField field = inputRowType.getField(fieldName, false, false);
        if (field == null) {
          field = inputRowType.getField(fieldName + "__ntz", false, false);
        }
        String result = (field != null) ? field.getName() : fieldName;
        return result;

      default:
        // For any other expression type not explicitly aliased,
        // pass null to let Calcite derive the field name (e.g., $f0, EXPR$1, or based on function
        // name).
        return null;
    }
  }
}
