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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.arrow.ArrowFieldTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.spark.connect.proto.LocalRelation;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.ShowString;

public class RelationToCalcite {

  // Do not instantiate
  private RelationToCalcite() {}

  static RelNode translateRelationToRel(Relation sparkRelation, RelBuilder relBuilder)
      throws IOException {
    switch (sparkRelation.getRelTypeCase()) {
      case LOCAL_RELATION:
        return translateLocalRelation(sparkRelation.getLocalRelation(), relBuilder);
      case SHOW_STRING:
        return translateShowString(sparkRelation.getShowString(), relBuilder);
      default:
        throw new UnsupportedOperationException("Relation not supported: " + sparkRelation);
    }
  }

  private static RelDataType arrowSchemaToRowType(Schema schema, JavaTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Field field : schema.getFields()) {
      builder.add(field.getName(), ArrowFieldTypeFactory.toType(field.getType(), typeFactory));
    }
    return builder.build();
  }

  private static void addRows(
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

  // Helper to create RexLiteral from Java Object based on RelDataType and ArrowType
  private static RexLiteral createRexLiteral(
      RexBuilder rexBuilder, Object javaValue, RelDataType relDataType, ArrowType arrowType) {
    if (javaValue == null) {
      return rexBuilder.makeNullLiteral(relDataType);
    }

    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();

    // TODO: check these details more closely
    switch (sqlTypeName) {
      case VARCHAR:
      case CHAR:
        return rexBuilder.makeLiteral(javaValue, relDataType);
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
        //            long epochMillis = convertArrowTimestampToMillis((Long) javaValue, arrowType);
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

  private static long convertArrowTimestampToMillis(long rawValue, ArrowType arrowType) {
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

  private static RelNode translateLocalRelation(LocalRelation localRelation, RelBuilder relBuilder)
      throws IOException {
    if (!localRelation.hasData()) {
      throw new UnsupportedOperationException(
          "LocalRelation must have `data` field. "
              + "Parsing Spark SQL DDL or JSON type representation is not supported yet.");
    }

    // Convert the incoming Arrow stream bytes to an ArrowFileReader since that is what Calcite
    // expects
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      ByteArrayInputStream bais = new ByteArrayInputStream(localRelation.getData().toByteArray());

      ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = ImmutableList.builder();

      try (ArrowStreamReader streamReader = new ArrowStreamReader(bais, allocator)) {
        VectorSchemaRoot root = streamReader.getVectorSchemaRoot();
        RelDataType rowType =
            arrowSchemaToRowType(root.getSchema(), (JavaTypeFactory) relBuilder.getTypeFactory());

        addRows(tuplesBuilder, relBuilder.getRexBuilder(), root, rowType);

        while (streamReader.loadNextBatch()) {
          // could/should assert the schema hasn't changed but whatever
          // the root is mutated by loadNextBatch
          addRows(tuplesBuilder, relBuilder.getRexBuilder(), root, rowType);
        }

        // Create a values node with that schema
        return relBuilder.values(rowType, 3).build();
      }
    }
  }

  private static RelNode translateShowString(ShowString showString, RelBuilder relBuilder)
      throws IOException {
    // For this we need an actual custom rel that will have a DoFn that does the "show" logic
    RelDataType rowType =
        relBuilder.getTypeFactory().builder().add("show_string", SqlTypeName.VARCHAR).build();
    return relBuilder.values(rowType, "this is totally fake").build();
  }
}
