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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSyntax;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.spark.connect.proto.CallFunction;
import org.apache.spark.connect.proto.Expression;

public class SparkExpressionToRexNode {
  private final RexBuilder rexBuilder;
  private final RelDataTypeFactory typeFactory;

  public SparkExpressionToRexNode(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
    this.typeFactory = rexBuilder.getTypeFactory();
  }

  public RexNode translate(Expression expr) {
    switch (expr.getExprTypeCase()) {
      case LITERAL:
        return translateLiteral(expr.getLiteral());
      case CALL_FUNCTION: // Or UNRESOLVED_FUNCTION
        return translateCallFunction(expr.getCallFunction());
      // TODO: Implement other expression types like ALIAS, WINDOW, etc.
      default:
        throw new UnsupportedOperationException("Spark Expression type not supported: " + expr.getExprTypeCase());
    }
  }

  // --- Conversion Methods for Expression Types ---

  private RexNode translateLiteral(Expression.Literal literal) {
    RelDataType type = sparkTypeToRelDataType(literal.getDataType()); // Use type from proto if available
    switch (literal.getLiteralTypeCase()) {
      case NULL:
        return rexBuilder.makeNullLiteral(sparkTypeToRelDataType(literal.getNull()));
      case BOOLEAN:
        return rexBuilder.makeLiteral(literal.getBoolean());
      case STRING:
        return rexBuilder.makeLiteral(literal.getString());
      case BYTE:
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(literal.getByte()), typeFactory.createSqlType(SqlTypeName.TINYINT));
      case SHORT:
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(literal.getShort()), typeFactory.createSqlType(SqlTypeName.SMALLINT));
      case INTEGER:
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(literal.getInteger()), typeFactory.createSqlType(SqlTypeName.INTEGER));
      case LONG:
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(literal.getLong()), typeFactory.createSqlType(SqlTypeName.BIGINT));
      case FLOAT:
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(literal.getFloat()), typeFactory.createSqlType(SqlTypeName.FLOAT));
      case DOUBLE:
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(literal.getDouble()), typeFactory.createSqlType(SqlTypeName.DOUBLE));
      case DECIMAL:
        return rexBuilder.makeExactLiteral(
          new BigDecimal(literal.getDecimal().getValue()),
          sparkTypeToRelDataType(literal.getDataType())); // DECIMAL type requires precision/scale
      case DATE:
        // Calcite DateLiteral from days since epoch
        return rexBuilder.makeDateLiteral(org.apache.calcite.util.DateString.fromDaysSinceEpoch(literal.getDate()));
      case TIMESTAMP:
        // Spark Timestamp is micros since epoch. Calcite is millis.
        long millis = literal.getTimestamp() / 1000;
        return rexBuilder.makeTimestampLiteral(org.apache.calcite.util.TimestampString.fromMillisSinceEpoch(millis), SqlTypeName.TIMESTAMP.getDefaultPrecision());
      // TODO: Handle other literal types: BINARY, ARRAY, MAP, STRUCT, INTERVALS
      default:
        throw new UnsupportedOperationException("Literal type not supported: " + literal.getLiteralTypeCase());
    }
  }

//  private RexNode translateUnresolvedAttribute(Expression.UnresolvedAttribute attr) {
//    String name = attr.getUnparsedIdentifier();
//    RelDataTypeField field = inputRowType.getField(name, false, false);
//    if (field == null) {
//      throw new IllegalArgumentException("Column not found: " + name + " in input schema: " + inputRowType);
//    }
//    return rexBuilder.makeInputRef(field.getType(), field.getIndex());
//  }

//  private RexNode translateCast(Expression.Cast cast) {
//    RexNode operand = translate(cast.getExpr());
//    RelDataType targetType;
//    if (cast.hasType()) {
//      targetType = sparkTypeToRelDataType(cast.getType());
//    } else {
//      // TODO: Support parsing type_str if necessary, though this is server-side
//      throw new UnsupportedOperationException("Casting to type_str is not supported in this translateer.");
//    }
//    return rexBuilder.makeCast(targetType, operand);
//  }

  private RexNode translateCallFunction(CallFunction func) {
    String funcName = func.getFunctionName();
    List<RexNode> operands = func.getArgumentsList().stream()
      .map(this::translate)
      .collect(Collectors.toList());

    // Lookup the operator in Calcite's tables
    // TODO: Enhance operator lookup (case-insensitivity, multiple tables)
    List<SqlOperator> operators = new java.util.ArrayList<>();
    SqlStdOperatorTable.instance().lookupOperatorOverloads(
      SqlIdentifier(funcName, SqlParserPos.ZERO),
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

    return rexBuilder.makeCall(operator, operands);
  }

  /**
   * Derives the output field names for a list of Spark Connect Expressions
   * to be used in a Calcite LogicalProject.
   *
   * @param sparkExpressions The list of expressions from the Project proto.
   * @return A list of strings, where each element is the desired field name
   *         or null if the name should be derived by Calcite.
   */
  public List<String> deriveFieldNames(List<Expression> sparkExpressions) {
    List<String> fieldNames = new ArrayList<>();
    for (Expression expr : sparkExpressions) {
      String name = null;
      switch (expr.getExprTypeCase()) {
        case ALIAS:
          List<String> aliasNames = expr.getAlias().getNameList();
          if (!aliasNames.isEmpty()) {
            // For scalar projections, there should be only one name part.
            name = aliasNames.get(0);
          }
          // If no name parts, fall through to default (null)
          break;

        case UNRESOLVED_ATTRIBUTE:
          // If an attribute is used directly in a projection without an alias,
          // its name becomes the output column name.
          String unparsed = expr.getUnresolvedAttribute().getUnparsedIdentifier();
          // Handle potential qualification (e.g., "table.col"). We usually want just the column name.
          int dotIndex = unparsed.lastIndexOf('.');
          name = (dotIndex == -1) ? unparsed : unparsed.substring(dotIndex + 1);
          break;

        case LITERAL:
        case UNRESOLVED_FUNCTION:
        case CALL_FUNCTION:
        case CAST:
          // ... other expression types ...
        default:
          // For any other expression type not explicitly aliased,
          // pass null to let Calcite derive the field name (e.g., $f0, EXPR$1, or based on function name).
          name = null;
          break;
      }
      fieldNames.add(name);
    }
    return fieldNames;
  }
}
