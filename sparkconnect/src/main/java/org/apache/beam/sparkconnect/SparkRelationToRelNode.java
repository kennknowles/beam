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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.beam.sparkconnect.rel.LogicalShowString;
import org.apache.beam.vendor.calcite.v1_40_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.arrow.ArrowFieldTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelCollations;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalSort;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalValues;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.spark.connect.proto.Aggregate;
import org.apache.spark.connect.proto.Deduplicate;
import org.apache.spark.connect.proto.Drop;
import org.apache.spark.connect.proto.Expression;
import org.apache.spark.connect.proto.Filter;
import org.apache.spark.connect.proto.Join;
import org.apache.spark.connect.proto.Limit;
import org.apache.spark.connect.proto.LocalRelation;
import org.apache.spark.connect.proto.Offset;
import org.apache.spark.connect.proto.Project;
import org.apache.spark.connect.proto.Range;
import org.apache.spark.connect.proto.Read;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SQL;
import org.apache.spark.connect.proto.SetOperation;
import org.apache.spark.connect.proto.ShowString;
import org.apache.spark.connect.proto.Sort;
import org.apache.spark.connect.proto.Tail;
import org.apache.spark.connect.proto.WithColumns;
import org.apache.spark.connect.proto.WithColumnsRenamed;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SparkRelationToRelNode {

  private final RelOptCluster cluster;

  public SparkRelationToRelNode(RelOptCluster cluster) {
    this.cluster = cluster;
  }

  public RelNode translate(Relation sparkRelation) {
    switch (sparkRelation.getRelTypeCase()) {
      case READ:
        return translateRead(sparkRelation.getRead());
      case PROJECT:
        return translateProject(sparkRelation.getProject());
      case FILTER:
        return translateFilter(sparkRelation.getFilter());
      case JOIN:
        return translateJoin(sparkRelation.getJoin());
      case SET_OP:
        return translateSetOp(sparkRelation.getSetOp());
      case SORT:
        return translateSort(sparkRelation.getSort());
      case LIMIT:
        return translateLimit(sparkRelation.getLimit());
      case OFFSET:
        return translateOffset(sparkRelation.getOffset());
      case AGGREGATE:
        return translateAggregate(sparkRelation.getAggregate());
      case LOCAL_RELATION:
        return translateLocalRelation(sparkRelation.getLocalRelation());
      case DEDUPLICATE:
        return translateDeduplicate(sparkRelation.getDeduplicate());
      case RANGE:
        return translateRange(sparkRelation.getRange());
      case SQL:
        return translateSql(sparkRelation.getSql());
      case TAIL:
        return translateTail(sparkRelation.getTail());
      case DROP:
        return translateDrop(sparkRelation.getDrop());
      case WITH_COLUMNS_RENAMED:
        return translateWithColumnsRenamed(sparkRelation.getWithColumnsRenamed());
      case WITH_COLUMNS:
        return translateWithColumns(sparkRelation.getWithColumns());
      case SHOW_STRING:
        return translateShowString(sparkRelation.getShowString());
      default:
        return unsupported(sparkRelation.getRelTypeCase().name());
    }
  }

  private RelNode unsupported(String typeName) {
    throw new UnsupportedOperationException("Spark Relation type not supported yet: " + typeName);
  }

  private RelNode translateShowString(ShowString showStringProto) {
    RelNode input = translate(showStringProto.getInput());
    return new LogicalShowString(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        input,
        showStringProto.getNumRows(),
        showStringProto.getTruncate(),
        showStringProto.getVertical());
  }

  private RelNode translateProject(Project projectProto) {
    RelNode inputNode;
    if (projectProto.hasInput()) {
      inputNode = translate(projectProto.getInput());
    } else {
      inputNode = LogicalValues.createOneRow(cluster);
    }
    RelDataType inputRowType = inputNode.getRowType();

    SparkExpressionToRexNode exprConverter = new SparkExpressionToRexNode(cluster, inputRowType);
    List<RexNode> calciteProjections =
        projectProto.getExpressionsList().stream()
            .map(exprConverter::translate)
            .collect(Collectors.toList());

    List<@Nullable String> fieldNames =
        exprConverter.deriveFieldNames(projectProto.getExpressionsList());

    return LogicalProject.create(
        inputNode, Collections.emptyList(), calciteProjections, fieldNames, Collections.emptySet());
  }

  private RelNode translateRead(Read readProto) {
    if (readProto.hasNamedTable()) {
      List<String> tableName =
          ImmutableList.copyOf(readProto.getNamedTable().getUnparsedIdentifier().split("\\."));
      // RelOptTable table = catalogReader.getTable(tableName);
      // if (table == null) {
      //     throw new IllegalArgumentException("Table not found: " +
      // readProto.getNamedTable().getUnparsedIdentifier());
      // }
      // return LogicalTableScan.create(cluster, table);
      return unsupported("Read NamedTable - requires catalog lookup");
    } else if (readProto.hasDataSource()) {
      return unsupported("Read DataSource");
    }
    throw new IllegalArgumentException("Invalid Read proto");
  }

  private RelNode translateFilter(Filter filterProto) {
    RelNode inputNode = translate(filterProto.getInput());
    SparkExpressionToRexNode exprConverter =
        new SparkExpressionToRexNode(cluster, inputNode.getRowType());
    RexNode condition = exprConverter.translate(filterProto.getCondition());
    return LogicalFilter.create(inputNode, condition);
  }

  private RelNode translateJoin(Join joinProto) {
    RelNode left = translate(joinProto.getLeft());
    RelNode right = translate(joinProto.getRight());

    RexNode condition;
    int leftFieldCount = left.getRowType().getFieldCount();

    if (joinProto.hasJoinCondition()) {
      RelDataType joinRowType =
          cluster.getTypeFactory().createJoinType(left.getRowType(), right.getRowType());
      // Need an expression translateer that can handle field references from both left and right
      // inputs
      SparkExpressionToRexNode joinExprConverter =
          new SparkExpressionToRexNode(cluster, joinRowType);
      condition = joinExprConverter.translate(joinProto.getJoinCondition());
    } else if (joinProto.getUsingColumnsCount() > 0) {
      List<RexNode> equiConditions = new ArrayList<>();
      for (String colName : joinProto.getUsingColumnsList()) {
        RelDataTypeField leftField = left.getRowType().getField(colName, false, false);
        RelDataTypeField rightField = right.getRowType().getField(colName, false, false);
        if (leftField == null)
          throw new IllegalArgumentException(
              "using_column " + colName + " not found in left join input");
        if (rightField == null)
          throw new IllegalArgumentException(
              "using_column " + colName + " not found in right join input");

        RexNode leftRef =
            cluster.getRexBuilder().makeInputRef(leftField.getType(), leftField.getIndex());
        RexNode rightRef =
            cluster
                .getRexBuilder()
                .makeInputRef(rightField.getType(), leftFieldCount + rightField.getIndex());
        equiConditions.add(
            cluster.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef));
      }
      // this might not be right - we should probably just build a RexNode matching the syntax and
      // save the join analysis for later
      condition = cluster.getRexBuilder().makeCall(SqlStdOperatorTable.AND, equiConditions);
    } else if (joinProto.getJoinType() == Join.JoinType.JOIN_TYPE_CROSS) {
      condition = cluster.getRexBuilder().makeLiteral(true);
    } else {
      throw new IllegalArgumentException(
          "Join must have a condition, using_columns, or be a CROSS join");
    }

    return LogicalJoin.create(
        left,
        right,
        ImmutableList.of(),
        condition,
        new HashSet<>(),
        translateSparkJoinType(joinProto.getJoinType()));
  }

  private JoinRelType translateSparkJoinType(Join.JoinType sparkJoinType) {
    switch (sparkJoinType) {
      case JOIN_TYPE_INNER:
        return JoinRelType.INNER;
      case JOIN_TYPE_LEFT_OUTER:
        return JoinRelType.LEFT;
      case JOIN_TYPE_RIGHT_OUTER:
        return JoinRelType.RIGHT;
      case JOIN_TYPE_FULL_OUTER:
        return JoinRelType.FULL;
      case JOIN_TYPE_LEFT_SEMI:
        return JoinRelType.SEMI;
      case JOIN_TYPE_LEFT_ANTI:
        return JoinRelType.ANTI;
      default:
        throw new UnsupportedOperationException("Spark JoinType not supported: " + sparkJoinType);
    }
  }

  private RelNode translateSetOp(SetOperation setOpProto) {
    RelNode left = translate(setOpProto.getLeftInput());
    RelNode right = translate(setOpProto.getRightInput());
    boolean all = setOpProto.getIsAll();
    // TODO: Handle by_name and allow_missing_columns for UNION
    switch (setOpProto.getSetOpType()) {
      case SET_OP_TYPE_UNION:
        return LogicalUnion.create(ImmutableList.of(left, right), all);
      case SET_OP_TYPE_INTERSECT:
        return LogicalIntersect.create(ImmutableList.of(left, right), all);
      case SET_OP_TYPE_EXCEPT:
        return LogicalMinus.create(ImmutableList.of(left, right), all);
      case SET_OP_TYPE_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw new UnsupportedOperationException(
            "SetOpType not supported: " + setOpProto.getSetOpType());
    }
  }

  private RelFieldCollation translateSortOrder(
      Expression.SortOrder sortOrderProto, int fieldIndex) {

    RelFieldCollation.Direction direction =
        sortOrderProto.getDirection() == Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
            ? RelFieldCollation.Direction.ASCENDING
            : RelFieldCollation.Direction.DESCENDING;
    RelFieldCollation.NullDirection nullDirection;
    switch (sortOrderProto.getNullOrdering()) {
      case SORT_NULLS_FIRST:
        nullDirection = RelFieldCollation.NullDirection.FIRST;
        break;
      case SORT_NULLS_LAST:
        nullDirection = RelFieldCollation.NullDirection.LAST;
        break;
      case UNRECOGNIZED:
        throw new UnsupportedOperationException(
            "Null ordering not recognized: " + sortOrderProto.getNullOrdering());

      case SORT_NULLS_UNSPECIFIED:
      default:
        nullDirection =
            RelFieldCollation.NullDirection.FIRST; // Spark's default - not leaving to Calcite!
    }
    return new RelFieldCollation(fieldIndex, direction, nullDirection);
  }

  private RelNode translateSort(Sort sortProto) {
    RelNode input = translate(sortProto.getInput());
    SparkExpressionToRexNode exprConverter =
        new SparkExpressionToRexNode(cluster, input.getRowType());

    List<RexNode> sortExprs = new ArrayList<>();
    List<RelFieldCollation> collations = new ArrayList<>();

    int fieldIndex = 0;
    for (Expression.SortOrder order : sortProto.getOrderList()) {
      ++fieldIndex;
      sortExprs.add(exprConverter.translate(order.getChild()));
      collations.add(translateSortOrder(order, fieldIndex));
    }

    LogicalProject projectedSortFields =
        LogicalProject.create(
            input, Collections.emptyList(), sortExprs, (List<String>) null, Collections.emptySet());

    // TODO: do we need to consider offsets?
    return LogicalSort.create(projectedSortFields, RelCollations.of(collations), null, null);
  }

  private RelNode translateLimit(Limit limitProto) {
    RelNode input = translate(limitProto.getInput());
    RexNode limit =
        cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(limitProto.getLimit()));
    return LogicalSort.create(input, RelCollations.EMPTY, null, limit);
  }

  private RelNode translateOffset(Offset offsetProto) {
    RelNode input = translate(offsetProto.getInput());
    RexNode offset =
        cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(offsetProto.getOffset()));
    return LogicalSort.create(input, RelCollations.EMPTY, offset, null);
  }

  private RelNode translateTail(Tail tailProto) {
    // TAIL N is equivalent to ORDER BY all columns DESC (for some stable order) and then LIMIT N,
    // but applied from the end. This is not standard SQL and hard to map directly.
    // Or, if total count is known, it's OFFSET (COUNT - N).
    return unsupported("Tail");
  }

  private RelNode translateAggregate(Aggregate aggProto) {
    RelNode input = translate(aggProto.getInput());
    return unsupported("Aggregate - Complex conversion");
  }

  private RelNode translateDeduplicate(Deduplicate dedupeProto) {
    return unsupported("Deduplicate");
    //    RelNode input = translate(dedupeProto.getInput());
    //    ImmutableIntList groupSet = ImmutableIntList.range(0, input.getRowType().getFieldCount());
    //    return LogicalAggregate.create(input, groupSet, ImmutableList.of(groupSet),
    // ImmutableList.of());
  }

  private RelNode translateRange(Range rangeProto) {
    long start = rangeProto.getStart();
    long end = rangeProto.getEnd();
    long step = rangeProto.getStep();
    if (step == 0) throw new IllegalArgumentException("Range step cannot be zero.");

    RelDataType rowType =
        cluster
            .getTypeFactory()
            .createStructType(
                ImmutableList.of(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
                ImmutableList.of("id"));

    ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
    if (step > 0) {
      for (long i = start; i < end; i += step) {
        tuples.add(
            ImmutableList.of(cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(i))));
      }
    } else { // step < 0
      for (long i = start; i > end; i += step) {
        tuples.add(
            ImmutableList.of(cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(i))));
      }
    }
    return LogicalValues.create(cluster, rowType, tuples.build());
  }

  private RelNode translateSql(SQL sqlProto) {
    return unsupported("SQL - requires SQL parsing pipeline");
  }

  private RelNode translateDrop(Drop dropProto) {
    return unsupported("Drop");
    //    RelNode input = translate(dropProto.getInput());
    //    List<String> dropNames = dropProto.getColumnNamesList();
    //    // TODO: Handle dropProto.getColumns() expressions
    //
    //    List<RexNode> projections = new ArrayList<>();
    //    List<String> newFieldNames = new ArrayList<>();
    //    for (RelDataTypeField field : input.getRowType().getFieldList()) {
    //      if (!dropNames.contains(field.getName())) {
    //        projections.add(cluster.getRexBuilder().makeInputRef(field.getType(),
    // field.getIndex()));
    //        newFieldNames.add(field.getName());
    //      }
    //    }
    //    return LogicalProject.create(input, projections, newFieldNames);
  }

  private RelNode translateWithColumnsRenamed(WithColumnsRenamed renameProto) {
    RelNode input = translate(renameProto.getInput());
    // TODO: Implement renaming
    return unsupported("WithColumnsRenamed");
  }

  private RelNode translateWithColumns(WithColumns withColumnsProto) {
    RelNode input = translate(withColumnsProto.getInput());
    // TODO: Implement adding/replacing columns
    return unsupported("WithColumns");
  }

  private RelDataType arrowSchemaToRowType(Schema schema, JavaTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Field field : schema.getFields()) {
      builder.add(field.getName(), ArrowFieldTypeFactory.toType(field.getType(), typeFactory));
    }
    return builder.build();
  }

  /**
   * Pivots column-oriented Arrow VectorSchemaRoot into Calcite-friendly literal rows. Not suitable
   * for large scale, but useful for small literal dataframes/relations.
   */
  private void addRows(
      ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder,
      RexBuilder rexBuilder,
      VectorSchemaRoot root,
      RelDataType rowType) {

    int rowCount = root.getRowCount();
    if (rowCount == 0) {
      return;
    }

    List<Field> arrowFields = root.getSchema().getFields();
    List<FieldVector> vectors = root.getFieldVectors();

    for (int i = 0; i < rowCount; i++) {
      ImmutableList.Builder<RexLiteral> rowBuilder = ImmutableList.builder();

      for (int j = 0; j < vectors.size(); j++) {
        FieldVector vector = vectors.get(j);
        Object javaValue = vector.getObject(i);

        RelDataType fieldType = rowType.getFieldList().get(j).getType();
        ArrowType arrowType = arrowFields.get(j).getType();

        RexLiteral literal = createRexLiteral(rexBuilder, javaValue, fieldType, arrowType);
        rowBuilder.add(literal);
      }
      tuplesBuilder.add(rowBuilder.build());
    }
  }

  private RexLiteral createRexLiteral(
      RexBuilder rexBuilder, Object javaValue, RelDataType relDataType, ArrowType arrowType) {
    if (javaValue == null) {
      checkArgument(
          relDataType.isNullable(), "Received null arrow value for non-nullable Calcite type");
      return rexBuilder.makeNullLiteral(relDataType);
    }

    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();

    // TODO: check these details more closely
    switch (sqlTypeName) {
      case VARCHAR:
      case CHAR:
        // TODO: maybe there's a better way to toString an arrow Text object
        return rexBuilder.makeLiteral(javaValue.toString(), relDataType);
      case BOOLEAN:
        return rexBuilder.makeLiteral(javaValue, relDataType);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(((Number) javaValue).longValue()), relDataType);
      case FLOAT: // Calcite FLOAT is 4 bytes
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) javaValue).floatValue()));
      case REAL: // Typically synonym for FLOAT
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) javaValue).floatValue()));
      case DOUBLE:
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) javaValue).doubleValue()));
      case DECIMAL:
        return rexBuilder.makeExactLiteral((BigDecimal) javaValue, relDataType);
      case DATE:
        // Arrow DateDayVector -> Integer (days since epoch)
        //          if (javaValue instanceof Integer) {
        //            LocalDate date = LocalDate.ofEpochDay((Integer) javaValue);
        //            return rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch((int)
        // date.toEpochDay()));
        //          }
      case TIME:
        // Arrow Time(Nano/Micro/Milli/Sec)Vector -> Long
        // Calcite TIME literal (precision is for fractional seconds)
        // Example: TIME without timezone
        // Needs conversion from nanos/micros/etc. of day to MillisTimeString
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIME_TZ:
      case TIMESTAMP:
        // Arrow TimestampVector -> Long (unit depends on ArrowType)
        //          if (javaValue instanceof Long) {
        //            long epochMillis = translateArrowTimestampToMillis((Long) javaValue,
        // arrowType);
        //            // Preserve precision if specified in RelDataType
        //            return
        // rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(epochMillis),
        // relDataType.getPrecision());
        //          }
        //          break; // Fall through to throw
      case BINARY:
      case VARBINARY:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_TZ:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
      case NULL:
      case UNKNOWN:
      case ANY:
      case SYMBOL:
      case MULTISET:
      case ARRAY:
      case MAP:
      case DISTINCT:
      case STRUCTURED:
      case ROW:
      case OTHER:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case MEASURE:
      case FUNCTION:
      case SARG:
      case UUID:
      case VARIANT:
      default:
        throw new UnsupportedOperationException(
            "RexLiteral conversion not implemented for: "
                + sqlTypeName
                + " from Arrow type "
                + arrowType);
    }
  }

  private long translateArrowTimestampToMillis(long rawValue, ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
      switch (tsType.getUnit()) {
        case MILLISECOND:
          return rawValue;
        case MICROSECOND:
          return rawValue / 1000L;
        case NANOSECOND:
          return rawValue / 1000000L;
        case SECOND:
          return rawValue * 1000L;
      }
    }
    throw new IllegalArgumentException("Unsupported Timestamp unit in Arrow type: " + arrowType);
  }

  private RelNode translateLocalRelation(LocalRelation localRelation) {
    if (!localRelation.hasData()) {
      throw new UnsupportedOperationException(
          "LocalRelation must have `data` field. "
              + "Parsing Spark SQL DDL or JSON type representation is not supported.");
    }

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ByteArrayInputStream arrowBytesInputStream =
          new ByteArrayInputStream(localRelation.getData().toByteArray());

      ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = ImmutableList.builder();

      try (ArrowStreamReader streamReader =
          new ArrowStreamReader(arrowBytesInputStream, allocator)) {
        VectorSchemaRoot root = streamReader.getVectorSchemaRoot();
        RelDataType rowType =
            arrowSchemaToRowType(root.getSchema(), (JavaTypeFactory) cluster.getTypeFactory());
        addRows(tuplesBuilder, cluster.getRexBuilder(), root, rowType);
        while (streamReader.loadNextBatch()) {
          addRows(tuplesBuilder, cluster.getRexBuilder(), root, rowType);
        }

        return LogicalValues.create(cluster, rowType, tuplesBuilder.build());
      } catch (IOException exc) {
        throw new RuntimeException("Failed to parse arrow data for LocalRelation", exc);
      }
    }
  }
}
