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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;

/** Utility class to convert Beam {@link Row}s to Arrow {@link VectorSchemaRoot}. */
public final class RowToArrowConverter {

  private RowToArrowConverter() {}

  /** Converts a Beam {@link org.apache.beam.sdk.schemas.Schema} to an Arrow {@link Schema}. */
  public static Schema toArrowSchema(org.apache.beam.sdk.schemas.Schema beamSchema) {
    List<Field> arrowFields = new ArrayList<>();
    for (org.apache.beam.sdk.schemas.Schema.Field beamField : beamSchema.getFields()) {
      arrowFields.add(toBeamField(beamField));
    }
    return new Schema(arrowFields);
  }

  private static Field toBeamField(org.apache.beam.sdk.schemas.Schema.Field beamField) {
    org.apache.beam.sdk.schemas.Schema.FieldType type = beamField.getType();
    ArrowType arrowType;

    if (type.getTypeName() == TypeName.STRING) {
      arrowType = new ArrowType.Utf8();
    } else if (type.getTypeName() == TypeName.INT32) {
      arrowType = new ArrowType.Int(32, true);
    } else if (type.getTypeName() == TypeName.INT64) {
      arrowType = new ArrowType.Int(64, true);
    } else if (type.getTypeName() == TypeName.FLOAT) {
      arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else if (type.getTypeName() == TypeName.DOUBLE) {
      arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    } else if (type.getTypeName() == TypeName.BOOLEAN) {
      arrowType = new ArrowType.Bool();
    } else if (type.getTypeName() == TypeName.BYTES) {
      arrowType = new ArrowType.Binary();
    } else if (type.getTypeName() == TypeName.DATETIME) {
      arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    } else if (type.getTypeName() == TypeName.LOGICAL_TYPE) {
      org.apache.beam.sdk.schemas.Schema.LogicalType<?, ?> logicalType = type.getLogicalType();
      if (logicalType != null && "beam:logical_type:date:v1".equals(logicalType.getIdentifier())) {
        arrowType = new ArrowType.Date(DateUnit.DAY);
      } else {
        arrowType = new ArrowType.Utf8();
      }
    } else {
      arrowType = new ArrowType.Utf8();
    }

    return new Field(
        beamField.getName(), FieldType.nullable(arrowType), java.util.Collections.emptyList());
  }

  /** Populates a {@link VectorSchemaRoot} with a list of Beam {@link Row}s. */
  public static void populateVectorSchemaRoot(
      VectorSchemaRoot arrowRoot, List<Row> rows, org.apache.beam.sdk.schemas.Schema beamSchema) {

    arrowRoot.setRowCount(rows.size());
    for (int i = 0; i < beamSchema.getFieldCount(); i++) {
      org.apache.beam.sdk.schemas.Schema.Field beamField = beamSchema.getField(i);
      FieldVector vector = arrowRoot.getVector(beamField.getName());
      vector.allocateNew();

      for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
        Row row = rows.get(rowIndex);
        Object value = row.getValue(i);

        if (value == null) {
          vector.setNull(rowIndex);
          continue;
        }

        if (vector instanceof VarCharVector) {
          String strValue;
          if (beamField.getType().getTypeName() == TypeName.STRING) {
            strValue = (String) value;
          } else {
            strValue = value.toString();
          }
          ((VarCharVector) vector).setSafe(rowIndex, strValue.getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof IntVector) {
          ((IntVector) vector).setSafe(rowIndex, ((Number) value).intValue());
        } else if (vector instanceof BigIntVector) {
          ((BigIntVector) vector).setSafe(rowIndex, ((Number) value).longValue());
        } else if (vector instanceof Float4Vector) {
          ((Float4Vector) vector).setSafe(rowIndex, ((Number) value).floatValue());
        } else if (vector instanceof Float8Vector) {
          ((Float8Vector) vector).setSafe(rowIndex, ((Number) value).doubleValue());
        } else if (vector instanceof BitVector) {
          ((BitVector) vector).setSafe(rowIndex, (Boolean) value ? 1 : 0);
        } else if (vector instanceof VarBinaryVector) {
          ((VarBinaryVector) vector).setSafe(rowIndex, (byte[]) value);
        } else if (vector instanceof TimeStampMicroTZVector) {
          org.joda.time.ReadableInstant dt = (org.joda.time.ReadableInstant) value;
          ((TimeStampMicroTZVector) vector).setSafe(rowIndex, dt.getMillis() * 1000L);
        } else if (vector instanceof DateDayVector) {
          Object orig = row.getValue(i);
          if (orig instanceof Integer) {
            ((DateDayVector) vector).setSafe(rowIndex, (Integer) orig);
          } else if (orig instanceof Long) {
            ((DateDayVector) vector).setSafe(rowIndex, ((Long) orig).intValue());
          } else if (orig instanceof java.time.LocalDate) {
            ((DateDayVector) vector)
                .setSafe(rowIndex, (int) ((java.time.LocalDate) orig).toEpochDay());
          } else {
            String className = orig == null ? "null" : orig.getClass().getName();
            throw new RuntimeException("Unsupported Object type for Date vector: " + className);
          }
        } else {
          ((VarCharVector) vector)
              .setSafe(rowIndex, value.toString().getBytes(StandardCharsets.UTF_8));
        }
      }
      vector.setValueCount(rows.size());
    }
  }
}
