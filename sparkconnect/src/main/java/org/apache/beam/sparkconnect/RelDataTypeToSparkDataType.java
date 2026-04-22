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

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.spark.connect.proto.DataType;

public class RelDataTypeToSparkDataType {

  // TODO fix this
  @SuppressWarnings("nullness")
  public DataType relDataTypeToSparkDataType(RelDataType relDataType) {
    DataType.Builder sparkTypeBuilder = DataType.newBuilder();

    switch (relDataType.getSqlTypeName()) {
      case VARCHAR:
        sparkTypeBuilder.setString(DataType.String.newBuilder().build());
        break;
      case CHAR:
        sparkTypeBuilder.setChar(
            DataType.Char.newBuilder().setLength(relDataType.getPrecision()).build());
        break;
      case BOOLEAN:
        sparkTypeBuilder.setBoolean(DataType.Boolean.newBuilder().build());
        break;
      case TINYINT:
        sparkTypeBuilder.setByte(DataType.Byte.newBuilder().build());
        break;
      case SMALLINT:
        sparkTypeBuilder.setShort(DataType.Short.newBuilder().build());
        break;
      case INTEGER:
        sparkTypeBuilder.setInteger(DataType.Integer.newBuilder().build());
        break;
      case BIGINT:
        sparkTypeBuilder.setLong(DataType.Long.newBuilder().build());
        break;
      case FLOAT:
        sparkTypeBuilder.setFloat(DataType.Float.newBuilder().build());
        break;
      case DOUBLE:
      case REAL: // REAL is often a synonym for DOUBLE in SQL dialects
        sparkTypeBuilder.setDouble(DataType.Double.newBuilder().build());
        break;
      case DECIMAL:
        sparkTypeBuilder.setDecimal(
            DataType.Decimal.newBuilder()
                .setScale(relDataType.getScale())
                .setPrecision(relDataType.getPrecision())
                .build());
        break;
      case DATE:
        sparkTypeBuilder.setDate(DataType.Date.newBuilder().build());
        break;
      case TIMESTAMP:
        sparkTypeBuilder.setTimestamp(DataType.Timestamp.newBuilder().build());
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        sparkTypeBuilder.setTimestampNtz(DataType.TimestampNTZ.newBuilder().build());
        break;
      case INTERVAL_DAY_SECOND:
        sparkTypeBuilder.setDayTimeInterval(DataType.DayTimeInterval.newBuilder().build());
        break;
      case INTERVAL_YEAR_MONTH:
        sparkTypeBuilder.setYearMonthInterval(DataType.YearMonthInterval.newBuilder().build());
        break;
      case BINARY:
      case VARBINARY:
        sparkTypeBuilder.setBinary(DataType.Binary.newBuilder().build());
        break;
      case ARRAY:
        DataType arrayElementType = relDataTypeToSparkDataType(relDataType.getComponentType());
        sparkTypeBuilder.setArray(
            DataType.Array.newBuilder()
                .setElementType(arrayElementType)
                .setContainsNull(relDataType.getComponentType().isNullable())
                .build());
        break;
      case MAP:
        DataType keyType = relDataTypeToSparkDataType(relDataType.getKeyType());
        DataType valueType = relDataTypeToSparkDataType(relDataType.getValueType());
        sparkTypeBuilder.setMap(
            DataType.Map.newBuilder()
                .setKeyType(keyType)
                .setValueType(valueType)
                .setValueContainsNull(relDataType.getValueType().isNullable())
                .build());
        break;
      case ROW: // Calcite's ROW is equivalent to Spark's Struct
        DataType.Struct.Builder structBuilder = DataType.Struct.newBuilder();
        for (RelDataTypeField field : relDataType.getFieldList()) {
          DataType fieldType = relDataTypeToSparkDataType(field.getType());
          structBuilder.addFields(
              DataType.StructField.newBuilder()
                  .setName(field.getName())
                  .setDataType(fieldType)
                  .setNullable(field.getType().isNullable())
                  .build());
        }
        sparkTypeBuilder.setStruct(structBuilder.build());
        break;
      case NULL:
        sparkTypeBuilder.setNull(DataType.NULL.newBuilder().build());
        break;
      default:
        throw new UnsupportedOperationException(
            "Calcite SqlTypeName not supported: " + relDataType.getSqlTypeName());
    }

    return sparkTypeBuilder.build();
  }
}
