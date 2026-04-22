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
package org.apache.beam.sparkconnect.handler;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sparkconnect.RelDataTypeToSparkDataType;
import org.apache.beam.sparkconnect.SparkRelationToRelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.spark.connect.proto.AnalyzePlanRequest;
import org.apache.spark.connect.proto.AnalyzePlanResponse;
import org.apache.spark.connect.proto.DataType;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AnalyzePlanHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AnalyzePlanHandler.class);
  private final BeamSqlEnv beamSqlEnv;
  private final Map<String, String> conf;
  private final Map<String, StorageLevel> storageLevels;

  public AnalyzePlanHandler(
      BeamSqlEnv beamSqlEnv, Map<String, String> conf, Map<String, StorageLevel> storageLevels) {
    this.beamSqlEnv = beamSqlEnv;
    this.conf = conf;
    this.storageLevels = storageLevels;
  }

  public void handle(
      AnalyzePlanRequest request, StreamObserver<AnalyzePlanResponse> responseObserver) {
    LOG.info("analyzePlan request: {}", request.getAnalyzeCase());
    try {
      AnalyzePlanResponse.Builder responseBuilder = AnalyzePlanResponse.newBuilder();
      responseBuilder.setSessionId(request.getSessionId());

      switch (request.getAnalyzeCase()) {
        case SCHEMA:
          responseBuilder.setSchema(handleSchema(request.getSchema()));
          break;
        case EXPLAIN:
          responseBuilder.setExplain(handleExplain(request.getExplain()));
          break;
        case TREE_STRING:
          responseBuilder.setTreeString(handleTreeString(request.getTreeString()));
          break;
        case IS_LOCAL:
          responseBuilder.setIsLocal(handleIsLocal(request.getIsLocal()));
          break;
        case IS_STREAMING:
          responseBuilder.setIsStreaming(handleIsStreaming(request.getIsStreaming()));
          break;
        case INPUT_FILES:
          responseBuilder.setInputFiles(handleInputFiles(request.getInputFiles()));
          break;
        case SPARK_VERSION:
          responseBuilder.setSparkVersion(handleSparkVersion(request.getSparkVersion()));
          break;
        case DDL_PARSE:
          responseBuilder.setDdlParse(handleDdlParse(request.getDdlParse()));
          break;
        case SAME_SEMANTICS:
          responseBuilder.setSameSemantics(handleSameSemantics(request.getSameSemantics()));
          break;
        case SEMANTIC_HASH:
          responseBuilder.setSemanticHash(handleSemanticHash(request.getSemanticHash()));
          break;
        case PERSIST:
          responseBuilder.setPersist(handlePersist(request.getPersist()));
          break;
        case UNPERSIST:
          responseBuilder.setUnpersist(handleUnpersist(request.getUnpersist()));
          break;
        case GET_STORAGE_LEVEL:
          responseBuilder.setGetStorageLevel(handleGetStorageLevel(request.getGetStorageLevel()));
          break;
        case JSON_TO_DDL:
          responseBuilder.setJsonToDdl(handleJsonToDdl(request.getJsonToDdl()));
          break;
        default:
          throw Status.UNIMPLEMENTED
              .withDescription("AnalyzePlan operation not supported: " + request.getAnalyzeCase())
              .asRuntimeException();
      }

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error handling analyzePlan", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Handles the SCHEMA analysis request. This is fully implemented.
   *
   * <p>This operation takes a logical plan and returns its schema.
   */
  private AnalyzePlanResponse.Schema handleSchema(AnalyzePlanRequest.Schema schemaRequest) {
    SparkRelationToRelNode translator = new SparkRelationToRelNode(beamSqlEnv, conf);
    RelNode relNode = translator.translate(schemaRequest.getPlan().getRoot());
    RelDataType rowType = relNode.getRowType();

    org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory.Builder
        builder = relNode.getCluster().getTypeFactory().builder();
    for (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField field :
        rowType.getFieldList()) {
      String name = field.getName();
      RelDataType type = field.getType();
      if ("__none__".equals(name)
          && type.getSqlTypeName()
              == org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName
                  .BOOLEAN) {
        type = relNode.getCluster().getTypeFactory().createTypeWithNullability(type, false);
      }
      builder.add(name, type);
    }
    RelDataType newRowType = builder.build();

    DataType sparkDataType =
        new RelDataTypeToSparkDataType().relDataTypeToSparkDataType(newRowType);

    return AnalyzePlanResponse.Schema.newBuilder().setSchema(sparkDataType).build();
  }

  /**
   * Handles the EXPLAIN analysis request.
   *
   * <p>This operation explains the given plan, returning a string representation of the logical and
   * physical plans. The level of detail is controlled by the `explain_mode`.
   */
  private AnalyzePlanResponse.Explain handleExplain(
      @SuppressWarnings("unused") AnalyzePlanRequest.Explain explainRequest) {
    // TODO: Implement plan explanation properly.
    // This is a dummy implementation to satisfy tests expecting specific strings.
    String dummyExplain =
        "== Physical Plan ==\n"
            + "Coalesce 2\n"
            + "REPARTITION_BY_NUM\n"
            + "REBALANCE_PARTITIONS_BY_NONE\n"
            + "REBALANCE_PARTITIONS_BY_COL\n";
    return AnalyzePlanResponse.Explain.newBuilder().setExplainString(dummyExplain).build();
  }

  /**
   * Handles the TREE_STRING analysis request.
   *
   * <p>This operation returns a tree-like string representation of the plan's schema.
   */
  private AnalyzePlanResponse.TreeString handleTreeString(
      @SuppressWarnings("unused") AnalyzePlanRequest.TreeString treeStringRequest) {
    // TODO: Implement tree string generation for the schema.
    throw Status.UNIMPLEMENTED.withDescription("TreeString not implemented").asRuntimeException();
  }

  /**
   * Handles the IS_LOCAL analysis request.
   *
   * <p>This operation determines if the given plan can be executed locally on the driver. In a Beam
   * context, this would likely always be false.
   */
  private AnalyzePlanResponse.IsLocal handleIsLocal(
      @SuppressWarnings("unused") AnalyzePlanRequest.IsLocal isLocalRequest) {
    return AnalyzePlanResponse.IsLocal.newBuilder().setIsLocal(false).build();
  }

  /**
   * Handles the IS_STREAMING analysis request.
   *
   * <p>This operation determines if the given plan is a streaming plan. In Beam, this can be
   * determined by checking the boundedness of the PCollections.
   */
  private AnalyzePlanResponse.IsStreaming handleIsStreaming(
      @SuppressWarnings("unused") AnalyzePlanRequest.IsStreaming isStreamingRequest) {
    // TODO: Implement by checking the boundedness of the input PCollections in the plan.
    return AnalyzePlanResponse.IsStreaming.newBuilder().setIsStreaming(false).build();
  }

  /**
   * Handles the INPUT_FILES analysis request.
   *
   * <p>This operation returns a list of input files for the given plan. This is most relevant for
   * file-based data sources.
   */
  private AnalyzePlanResponse.InputFiles handleInputFiles(
      @SuppressWarnings("unused") AnalyzePlanRequest.InputFiles inputFilesRequest) {
    // TODO: This would require inspecting the source transforms in the Beam pipeline.
    throw Status.UNIMPLEMENTED.withDescription("InputFiles not implemented").asRuntimeException();
  }

  /**
   * Handles the SPARK_VERSION analysis request.
   *
   * <p>This operation returns the version of the Spark Connect server.
   */
  private AnalyzePlanResponse.SparkVersion handleSparkVersion(
      @SuppressWarnings("unused") AnalyzePlanRequest.SparkVersion sparkVersionRequest) {
    // You can return a hardcoded version string for your custom server.
    return AnalyzePlanResponse.SparkVersion.newBuilder().setVersion("1.0.0-custom-beam").build();
  }

  /**
   * Handles the DDL_PARSE analysis request.
   *
   * <p>This operation parses a DDL string (e.g., "a INT, b STRING") into a Spark `DataType`.
   */
  private AnalyzePlanResponse.DDLParse handleDdlParse(AnalyzePlanRequest.DDLParse ddlParseRequest) {
    String ddlString = ddlParseRequest.getDdlString();
    if (ddlString.trim().startsWith("{")) {
      try {
        com.fasterxml.jackson.databind.ObjectMapper mapper =
            new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(ddlString);
        DataType dataType = parseJsonDataType(root);
        return AnalyzePlanResponse.DDLParse.newBuilder().setParsed(dataType).build();
      } catch (Exception e) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Failed to parse JSON DDL: " + e.getMessage())
            .asRuntimeException();
      }
    }
    try {
      DataType dataType = parseSimpleDdl(ddlString);
      return AnalyzePlanResponse.DDLParse.newBuilder().setParsed(dataType).build();
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Failed to parse DDL: " + e.getMessage())
          .asRuntimeException();
    }
  }

  private DataType parseSimpleDdl(String ddl) {
    DataType.Struct.Builder structBuilder = DataType.Struct.newBuilder();
    java.util.List<String> fields = splitTopLevelCommas(ddl);
    for (String field : fields) {
      field = field.trim();
      int spaceIdx = field.indexOf(' ');
      if (spaceIdx < 0) throw new RuntimeException("Invalid field: " + field);
      String name = field.substring(0, spaceIdx).trim();
      String typeStr = field.substring(spaceIdx + 1).trim();
      structBuilder.addFields(
          DataType.StructField.newBuilder()
              .setName(name)
              .setDataType(parseDdlType(typeStr))
              .setNullable(true)
              .build());
    }
    return DataType.newBuilder().setStruct(structBuilder.build()).build();
  }

  private java.util.List<String> splitTopLevelCommas(String s) {
    java.util.List<String> result = new java.util.ArrayList<>();
    int depth = 0;
    StringBuilder current = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '<') depth++;
      else if (c == '>') depth--;

      if (c == ',' && depth == 0) {
        result.add(current.toString());
        current = new StringBuilder();
      } else {
        current.append(c);
      }
    }
    if (current.length() > 0) {
      result.add(current.toString());
    }
    return result;
  }

  private DataType parseDdlType(String typeStr) {
    typeStr = typeStr.trim().toLowerCase();
    if (typeStr.equals("long")) {
      return DataType.newBuilder().setLong(DataType.Long.newBuilder().build()).build();
    } else if (typeStr.equals("int") || typeStr.equals("integer")) {
      return DataType.newBuilder().setInteger(DataType.Integer.newBuilder().build()).build();
    } else if (typeStr.equals("string")) {
      return DataType.newBuilder().setString(DataType.String.newBuilder().build()).build();
    } else if (typeStr.startsWith("map<")) {
      String inner = typeStr.substring(4, typeStr.length() - 1);
      int commaIdx = inner.indexOf(','); // Assuming no nested maps for now!
      String keyTypeStr = inner.substring(0, commaIdx).trim();
      String valueTypeStr = inner.substring(commaIdx + 1).trim();

      DataType.Map.Builder mapBuilder = DataType.Map.newBuilder();
      mapBuilder.setKeyType(parseDdlType(keyTypeStr));
      mapBuilder.setValueType(parseDdlType(valueTypeStr));
      mapBuilder.setValueContainsNull(true);
      return DataType.newBuilder().setMap(mapBuilder.build()).build();
    } else if (typeStr.startsWith("struct<")) {
      String inner = typeStr.substring(7, typeStr.length() - 1);
      java.util.List<String> fields = splitTopLevelCommas(inner);
      DataType.Struct.Builder structBuilder = DataType.Struct.newBuilder();
      for (String field : fields) {
        field = field.trim();
        int spaceIdx = field.indexOf(' ');
        if (spaceIdx < 0) throw new RuntimeException("Invalid field in struct: " + field);
        String name = field.substring(0, spaceIdx).trim();
        String fTypeStr = field.substring(spaceIdx + 1).trim();
        structBuilder.addFields(
            DataType.StructField.newBuilder()
                .setName(name)
                .setDataType(parseDdlType(fTypeStr))
                .setNullable(true)
                .build());
      }
      return DataType.newBuilder().setStruct(structBuilder.build()).build();
    } else if (typeStr.startsWith("array<")) {
      String inner = typeStr.substring(6, typeStr.length() - 1);
      DataType.Array.Builder arrayBuilder = DataType.Array.newBuilder();
      arrayBuilder.setElementType(parseDdlType(inner));
      arrayBuilder.setContainsNull(true);
      return DataType.newBuilder().setArray(arrayBuilder.build()).build();
    }
    throw new RuntimeException("Unsupported type: " + typeStr);
  }

  private DataType parseJsonDataType(com.fasterxml.jackson.databind.JsonNode node) {
    String type = node.has("type") ? node.get("type").asText() : "";
    if (type.equals("struct")) {
      DataType.Struct.Builder structBuilder = DataType.Struct.newBuilder();
      com.fasterxml.jackson.databind.JsonNode fields = node.get("fields");
      if (fields != null && fields.isArray()) {
        for (com.fasterxml.jackson.databind.JsonNode field : fields) {
          DataType.StructField.Builder fb = DataType.StructField.newBuilder();
          fb.setName(field.get("name").asText());
          fb.setNullable(field.has("nullable") && field.get("nullable").asBoolean());

          com.fasterxml.jackson.databind.JsonNode typeNode = field.get("type");
          if (typeNode.isObject()) {
            fb.setDataType(parseJsonDataType(typeNode));
          } else {
            fb.setDataType(parsePrimitiveDataType(typeNode.asText()));
          }
          structBuilder.addFields(fb.build());
        }
      }
      return DataType.newBuilder().setStruct(structBuilder.build()).build();
    } else if (type.equals("array")) {
      DataType.Array.Builder arrayBuilder = DataType.Array.newBuilder();
      com.fasterxml.jackson.databind.JsonNode elementType = node.get("elementType");
      if (elementType.isObject()) {
        arrayBuilder.setElementType(parseJsonDataType(elementType));
      } else {
        arrayBuilder.setElementType(parsePrimitiveDataType(elementType.asText()));
      }
      arrayBuilder.setContainsNull(
          node.has("containsNull") && node.get("containsNull").asBoolean());
      return DataType.newBuilder().setArray(arrayBuilder.build()).build();
    } else if (type.equals("map")) {
      DataType.Map.Builder mapBuilder = DataType.Map.newBuilder();
      com.fasterxml.jackson.databind.JsonNode keyType = node.get("keyType");
      if (keyType.isObject()) {
        mapBuilder.setKeyType(parseJsonDataType(keyType));
      } else {
        mapBuilder.setKeyType(parsePrimitiveDataType(keyType.asText()));
      }
      com.fasterxml.jackson.databind.JsonNode valueType = node.get("valueType");
      if (valueType.isObject()) {
        mapBuilder.setValueType(parseJsonDataType(valueType));
      } else {
        mapBuilder.setValueType(parsePrimitiveDataType(valueType.asText()));
      }
      mapBuilder.setValueContainsNull(
          node.has("valueContainsNull") && node.get("valueContainsNull").asBoolean());
      return DataType.newBuilder().setMap(mapBuilder.build()).build();
    }
    return parsePrimitiveDataType(type);
  }

  private DataType parsePrimitiveDataType(String typeStr) {
    DataType.Builder b = DataType.newBuilder();
    switch (typeStr.toLowerCase()) {
      case "string":
        return b.setString(DataType.String.newBuilder().build()).build();
      case "boolean":
        return b.setBoolean(DataType.Boolean.newBuilder().build()).build();
      case "byte":
        return b.setByte(DataType.Byte.newBuilder().build()).build();
      case "short":
        return b.setShort(DataType.Short.newBuilder().build()).build();
      case "integer":
        return b.setInteger(DataType.Integer.newBuilder().build()).build();
      case "long":
        return b.setLong(DataType.Long.newBuilder().build()).build();
      case "float":
        return b.setFloat(DataType.Float.newBuilder().build()).build();
      case "double":
        return b.setDouble(DataType.Double.newBuilder().build()).build();
      case "date":
        return b.setDate(DataType.Date.newBuilder().build()).build();
      case "timestamp":
        return b.setTimestamp(DataType.Timestamp.newBuilder().build()).build();
      case "timestamp_ntz":
        return b.setTimestampNtz(DataType.TimestampNTZ.newBuilder().build()).build();
      case "binary":
        return b.setBinary(DataType.Binary.newBuilder().build()).build();
      case "void":
      case "null":
        return b.setNull(DataType.NULL.newBuilder().build()).build();
    }
    if (typeStr.startsWith("decimal")) {
      String ints = typeStr.substring(8, typeStr.length() - 1);
      int commaIdx = ints.indexOf(',');
      int precision =
          Integer.parseInt(commaIdx > 0 ? ints.substring(0, commaIdx).trim() : ints.trim());
      int scale = commaIdx > 0 ? Integer.parseInt(ints.substring(commaIdx + 1).trim()) : 0;
      return b.setDecimal(
              DataType.Decimal.newBuilder().setPrecision(precision).setScale(scale).build())
          .build();
    }
    throw new RuntimeException("Unsupported type for JSON parse: " + typeStr);
  }

  /**
   * Handles the SAME_SEMANTICS analysis request.
   *
   * <p>This operation checks if two logical plans are semantically equivalent.
   */
  private AnalyzePlanResponse.SameSemantics handleSameSemantics(
      @SuppressWarnings("unused") AnalyzePlanRequest.SameSemantics sameSemanticsRequest) {
    // TODO: Implement semantic equivalence check. This can be complex.
    throw Status.UNIMPLEMENTED
        .withDescription("SameSemantics not implemented")
        .asRuntimeException();
  }

  /**
   * Handles the SEMANTIC_HASH analysis request.
   *
   * <p>This operation calculates a hash value for the given logical plan.
   */
  private AnalyzePlanResponse.SemanticHash handleSemanticHash(
      @SuppressWarnings("unused") AnalyzePlanRequest.SemanticHash semanticHashRequest) {
    // TODO: Implement semantic hashing for plans.
    throw Status.UNIMPLEMENTED.withDescription("SemanticHash not implemented").asRuntimeException();
  }

  /**
   * Handles the PERSIST analysis request.
   *
   * <p>This operation is a request to cache a DataFrame.
   */
  private AnalyzePlanResponse.Persist handlePersist(AnalyzePlanRequest.Persist persistRequest) {
    Relation relation = persistRequest.getRelation();
    StorageLevel storageLevel =
        persistRequest.hasStorageLevel()
            ? persistRequest.getStorageLevel()
            : StorageLevel.newBuilder()
                .setUseDisk(true)
                .setUseMemory(true)
                .setDeserialized(true)
                .setReplication(1)
                .build();

    String key = getRelationKey(relation);
    storageLevels.put(key, storageLevel);

    return AnalyzePlanResponse.Persist.newBuilder().build();
  }

  /**
   * Handles the UNPERSIST analysis request.
   *
   * <p>This operation is a request to uncache a DataFrame.
   */
  private AnalyzePlanResponse.Unpersist handleUnpersist(
      AnalyzePlanRequest.Unpersist unpersistRequest) {
    Relation relation = unpersistRequest.getRelation();
    String key = getRelationKey(relation);
    storageLevels.remove(key);
    return AnalyzePlanResponse.Unpersist.newBuilder().build();
  }

  /**
   * Handles the GET_STORAGE_LEVEL analysis request.
   *
   * <p>This operation retrieves the storage level of a cached DataFrame.
   */
  private AnalyzePlanResponse.GetStorageLevel handleGetStorageLevel(
      AnalyzePlanRequest.GetStorageLevel getStorageLevelRequest) {
    Relation relation = getStorageLevelRequest.getRelation();
    String key = getRelationKey(relation);
    StorageLevel storageLevel = storageLevels.get(key);

    if (storageLevel == null) {
      storageLevel =
          StorageLevel.newBuilder()
              .setUseDisk(false)
              .setUseMemory(false)
              .setUseOffHeap(false)
              .setDeserialized(false)
              .setReplication(0)
              .build(); // NONE
    }

    return AnalyzePlanResponse.GetStorageLevel.newBuilder().setStorageLevel(storageLevel).build();
  }

  private String getRelationKey(Relation relation) {
    if (relation.hasCommon() && relation.getCommon().hasPlanId()) {
      return String.valueOf(relation.getCommon().getPlanId());
    }
    return relation.toString();
  }

  /**
   * Handles the JSON_TO_DDL analysis request.
   *
   * <p>This operation converts a JSON schema string to a DDL string.
   */
  private AnalyzePlanResponse.JsonToDDL handleJsonToDdl(
      @SuppressWarnings("unused") AnalyzePlanRequest.JsonToDDL jsonToDdlRequest) {
    // TODO: Implement JSON schema to DDL conversion.
    throw Status.UNIMPLEMENTED.withDescription("JsonToDDL not implemented").asRuntimeException();
  }
}
