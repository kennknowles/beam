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
import java.util.Map;
import javax.annotation.Nullable;
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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
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

  @SuppressWarnings("nullness")
  private static Field toBeamField(org.apache.beam.sdk.schemas.Schema.Field beamField) {
    org.apache.beam.sdk.schemas.Schema.FieldType type = beamField.getType();
    ArrowType arrowType;
    List<Field> children = java.util.Collections.emptyList();

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
    } else if (type.getTypeName() == TypeName.ROW) {
      org.apache.beam.sdk.schemas.Schema schema = type.getRowSchema();
      if (schema == null) {
        throw new NullPointerException("schema is null");
      }
      arrowType = new ArrowType.Struct();
      children = new java.util.ArrayList<>();
      for (org.apache.beam.sdk.schemas.Schema.Field field : schema.getFields()) {
        children.add(toBeamField(field));
      }
    } else if (type.getTypeName() == TypeName.ARRAY) {
      org.apache.beam.sdk.schemas.Schema.FieldType componentType = type.getCollectionElementType();
      if (componentType == null) {
        throw new NullPointerException("componentType is null");
      }

      // Check if it represents a Map!
      if (componentType.getTypeName() == TypeName.ROW) {
        org.apache.beam.sdk.schemas.Schema rowSchema = componentType.getRowSchema();
        if (rowSchema == null) {
          throw new NullPointerException("rowSchema is null");
        }
        if (rowSchema.getFieldCount() == 2
            && "key".equals(rowSchema.getField(0).getName())
            && "value".equals(rowSchema.getField(1).getName())) {

          org.apache.beam.sdk.schemas.Schema.FieldType keyType = rowSchema.getField(0).getType();
          org.apache.beam.sdk.schemas.Schema.FieldType valueType = rowSchema.getField(1).getType();

          Field keyFieldNullable =
              toBeamField(org.apache.beam.sdk.schemas.Schema.Field.of("key", keyType));
          Field keyField =
              new Field(
                  keyFieldNullable.getName(),
                  new FieldType(false, keyFieldNullable.getType(), null),
                  keyFieldNullable.getChildren());
          Field valueField =
              toBeamField(org.apache.beam.sdk.schemas.Schema.Field.of("value", valueType));
          Field structField =
              new Field(
                  "entries",
                  new FieldType(false, new ArrowType.Struct(), null),
                  java.util.Arrays.asList(keyField, valueField));
          arrowType = new ArrowType.Map(false);
          children = java.util.Collections.singletonList(structField);
        } else {
          Field elementField =
              toBeamField(org.apache.beam.sdk.schemas.Schema.Field.of("item", componentType));
          arrowType = new ArrowType.List();
          children = java.util.Collections.singletonList(elementField);
        }
      } else {
        Field elementField =
            toBeamField(org.apache.beam.sdk.schemas.Schema.Field.of("item", componentType));
        arrowType = new ArrowType.List();
        children = java.util.Collections.singletonList(elementField);
      }
    } else if (type.getTypeName() == TypeName.MAP) {
      org.apache.beam.sdk.schemas.Schema.FieldType keyType = type.getMapKeyType();
      org.apache.beam.sdk.schemas.Schema.FieldType valueType = type.getMapValueType();
      if (keyType == null || valueType == null) {
        throw new NullPointerException("keyType or valueType is null");
      }
      Field keyFieldNullable =
          toBeamField(org.apache.beam.sdk.schemas.Schema.Field.of("key", keyType));
      Field keyField =
          new Field(
              keyFieldNullable.getName(),
              new FieldType(false, keyFieldNullable.getType(), null),
              keyFieldNullable.getChildren());
      Field valueField =
          toBeamField(org.apache.beam.sdk.schemas.Schema.Field.of("value", valueType));
      Field structField =
          new Field(
              "entries",
              new FieldType(false, new ArrowType.Struct(), null),
              java.util.Arrays.asList(keyField, valueField));
      arrowType = new ArrowType.Map(false);
      children = java.util.Collections.singletonList(structField);
    } else {
      arrowType = new ArrowType.Utf8();
    }

    return new Field(beamField.getName(), FieldType.nullable(arrowType), children);
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
        setVectorValue(vector, rowIndex, value, beamField.getType());
      }
      vector.setValueCount(rows.size());
    }
  }

  private static void setVectorValue(
      FieldVector vector,
      int index,
      @Nullable Object value,
      org.apache.beam.sdk.schemas.Schema.FieldType type) {
    System.out.println(
        "setVectorValue: vector="
            + vector.getClass().getName()
            + ", type="
            + type.getTypeName()
            + ", value="
            + value);
    if (value == null) {
      vector.setNull(index);
      return;
    }

    if (vector instanceof VarCharVector) {
      String strValue;
      if (type.getTypeName() == TypeName.STRING) {
        strValue = (String) value;
      } else {
        strValue = value.toString();
      }
      ((VarCharVector) vector).setSafe(index, strValue.getBytes(StandardCharsets.UTF_8));
    } else if (vector instanceof IntVector) {
      ((IntVector) vector).setSafe(index, ((Number) value).intValue());
    } else if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
    } else if (vector instanceof Float4Vector) {
      ((Float4Vector) vector).setSafe(index, ((Number) value).floatValue());
    } else if (vector instanceof Float8Vector) {
      ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
    } else if (vector instanceof BitVector) {
      ((BitVector) vector).setSafe(index, (Boolean) value ? 1 : 0);
    } else if (vector instanceof VarBinaryVector) {
      ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
    } else if (vector instanceof TimeStampMicroTZVector) {
      org.joda.time.ReadableInstant dt = (org.joda.time.ReadableInstant) value;
      ((TimeStampMicroTZVector) vector).setSafe(index, dt.getMillis() * 1000L);
    } else if (vector instanceof DateDayVector) {
      if (value instanceof Integer) {
        ((DateDayVector) vector).setSafe(index, (Integer) value);
      } else if (value instanceof Long) {
        ((DateDayVector) vector).setSafe(index, ((Long) value).intValue());
      } else if (value instanceof java.time.LocalDate) {
        ((DateDayVector) vector).setSafe(index, (int) ((java.time.LocalDate) value).toEpochDay());
      } else {
        throw new RuntimeException(
            "Unsupported Object type for Date vector: " + value.getClass().getName());
      }
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      List<?> list = (List<?>) value;
      org.apache.beam.sdk.schemas.Schema.FieldType elementType = type.getCollectionElementType();
      if (elementType == null) {
        throw new NullPointerException("elementType is null");
      }

      listVector.startNewValue(index);
      int offset = listVector.getOffsetBuffer().getInt(index * 4);
      for (int j = 0; j < list.size(); j++) {
        setVectorValue(listVector.getDataVector(), offset + j, list.get(j), elementType);
      }
      listVector.endValue(index, list.size());
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;

      mapVector.startNewValue(index);
      int offset = mapVector.getOffsetBuffer().getInt(index * 4);

      StructVector structVector = (StructVector) mapVector.getDataVector();
      int size = 0;

      if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        size = map.size();
        org.apache.beam.sdk.schemas.Schema.FieldType keyType = type.getMapKeyType();
        org.apache.beam.sdk.schemas.Schema.FieldType valueType = type.getMapValueType();
        if (keyType == null || valueType == null) {
          throw new NullPointerException("keyType or valueType is null");
        }

        int j = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          int elementIndex = offset + j;
          FieldVector keyVector = structVector.getChild("key");
          FieldVector valueVector = structVector.getChild("value");

          setVectorValue(keyVector, elementIndex, entry.getKey(), keyType);
          setVectorValue(valueVector, elementIndex, entry.getValue(), valueType);
          j++;
        }
      } else if (value instanceof List) {
        List<?> list = (List<?>) value;
        size = list.size();
        org.apache.beam.sdk.schemas.Schema.FieldType elementType = type.getCollectionElementType();
        if (elementType == null) {
          throw new NullPointerException("elementType is null");
        }
        org.apache.beam.sdk.schemas.Schema entrySchema = elementType.getRowSchema();
        if (entrySchema == null) {
          throw new NullPointerException("entrySchema is null");
        }
        org.apache.beam.sdk.schemas.Schema.FieldType keyType =
            entrySchema.getField("key").getType();
        org.apache.beam.sdk.schemas.Schema.FieldType valueType =
            entrySchema.getField("value").getType();

        for (int j = 0; j < list.size(); j++) {
          org.apache.beam.sdk.values.Row entryRow = (org.apache.beam.sdk.values.Row) list.get(j);
          if (entryRow == null) {
            throw new NullPointerException("entryRow is null");
          }
          int elementIndex = offset + j;
          FieldVector keyVector = structVector.getChild("key");
          FieldVector valueVector = structVector.getChild("value");

          setVectorValue(keyVector, elementIndex, entryRow.getValue("key"), keyType);
          setVectorValue(valueVector, elementIndex, entryRow.getValue("value"), valueType);
        }
      }
      mapVector.endValue(index, size);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      org.apache.beam.sdk.values.Row row = (org.apache.beam.sdk.values.Row) value;
      org.apache.beam.sdk.schemas.Schema schema = type.getRowSchema();
      if (schema == null) {
        throw new NullPointerException("schema is null");
      }
      structVector.setIndexDefined(index);
      for (int i = 0; i < schema.getFieldCount(); i++) {
        String fieldName = schema.getField(i).getName();
        FieldVector childVector = structVector.getChild(fieldName);
        setVectorValue(childVector, index, row.getValue(i), schema.getField(i).getType());
      }
    } else {
      ((VarCharVector) vector).setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
    }
  }
}
