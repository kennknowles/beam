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
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.spark.connect.proto.DataType;

public class SparkDataTypeToRelDataType {
  private final RelDataTypeFactory typeFactory; // *** CHANGED ***

  // *** MODIFIED CONSTRUCTOR ***
  public SparkDataTypeToRelDataType(RelDataTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
  }

  public RelDataType sparkDataTypeToRelDataType(DataType sparkType) {
    RelDataType baseType;
    switch (sparkType.getKindCase()) {
      case STRING:
        baseType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        break;
      case BOOLEAN:
        baseType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        break;
      case BYTE:
        baseType = typeFactory.createSqlType(SqlTypeName.TINYINT);
        break;
      case SHORT:
        baseType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
        break;
      case INTEGER:
        baseType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        break;
      case LONG:
        baseType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        break;
      case FLOAT:
        baseType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        break;
      case DOUBLE:
        baseType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        break;
      case DECIMAL:
        baseType =
            typeFactory.createSqlType(
                SqlTypeName.DECIMAL,
                sparkType.getDecimal().getPrecision(),
                sparkType.getDecimal().getScale());
        break;
      case DATE:
        baseType = typeFactory.createSqlType(SqlTypeName.DATE);
        break;
      case TIMESTAMP:
        baseType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        break;
      case TIMESTAMP_NTZ:
        baseType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        break;
      case BINARY:
        baseType = typeFactory.createSqlType(SqlTypeName.VARBINARY);
        break;
      case ARRAY:
        RelDataType elementType = sparkDataTypeToRelDataType(sparkType.getArray().getElementType());
        baseType = typeFactory.createArrayType(elementType, -1);
        break;
      case MAP:
        RelDataType keyType = sparkDataTypeToRelDataType(sparkType.getMap().getKeyType());
        RelDataType valueType = sparkDataTypeToRelDataType(sparkType.getMap().getValueType());
        baseType = typeFactory.createMapType(keyType, valueType);
        break;
      case STRUCT:
        RelDataTypeFactory.Builder structBuilder = typeFactory.builder();
        for (DataType.StructField field : sparkType.getStruct().getFieldsList()) {
          structBuilder.add(
              field.getName(),
              typeFactory.createTypeWithNullability(
                  sparkDataTypeToRelDataType(field.getDataType()), field.getNullable()));
        }
        baseType = structBuilder.build();
        break;
      case NULL:
        baseType = typeFactory.createSqlType(SqlTypeName.NULL);
        break;
      default:
        throw new UnsupportedOperationException(
            "Spark DataType not supported: " + sparkType.getKindCase());
    }

    // Spark Connect types do not have composable nullability, but instead attach it to fields
    // within structs.
    return typeFactory.createTypeWithNullability(baseType, false);
  }
}
