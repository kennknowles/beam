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
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sparkconnect.RelDataTypeToSparkDataType;
import org.apache.beam.sparkconnect.SparkRelationToRelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.spark.connect.proto.AnalyzePlanRequest;
import org.apache.spark.connect.proto.AnalyzePlanResponse;
import org.apache.spark.connect.proto.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AnalyzePlanHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AnalyzePlanHandler.class);

  private final BeamSqlEnv beamSqlEnv;

  public AnalyzePlanHandler(BeamSqlEnv beamSqlEnv) {
    this.beamSqlEnv = beamSqlEnv;
  }

  public void handle(
      AnalyzePlanRequest request, StreamObserver<AnalyzePlanResponse> responseObserver) {
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
      responseObserver.onError(e);
    }
  }

  /**
   * Handles the SCHEMA analysis request. This is fully implemented.
   *
   * <p>This operation takes a logical plan and returns its schema.
   */
  private AnalyzePlanResponse.Schema handleSchema(AnalyzePlanRequest.Schema schemaRequest) {
    SparkRelationToRelNode translator =
        new SparkRelationToRelNode(beamSqlEnv.getRelBuilder().getCluster());
    RelNode relNode = translator.translate(schemaRequest.getPlan().getRoot());
    RelDataType rowType = relNode.getRowType();

    // You will need a converter from Calcite's RelDataType to Spark Connect's DataType proto.
    // This is the inverse of the SparkDataTypeToRelDataType you created earlier.
    DataType sparkDataType = new RelDataTypeToSparkDataType().relDataTypeToSparkDataType(rowType);

    return AnalyzePlanResponse.Schema.newBuilder().setSchema(sparkDataType).build();
  }

  /**
   * Handles the EXPLAIN analysis request.
   *
   * <p>This operation explains the given plan, returning a string representation of the logical and
   * physical plans. The level of detail is controlled by the `explain_mode`.
   */
  private AnalyzePlanResponse.Explain handleExplain(AnalyzePlanRequest.Explain explainRequest) {
    // TODO: Implement plan explanation. This would involve running the Calcite planner
    // and then using a pretty-printer to format the resulting logical and physical plans.
    throw Status.UNIMPLEMENTED.withDescription("Explain not implemented").asRuntimeException();
  }

  /**
   * Handles the TREE_STRING analysis request.
   *
   * <p>This operation returns a tree-like string representation of the plan's schema.
   */
  private AnalyzePlanResponse.TreeString handleTreeString(
      AnalyzePlanRequest.TreeString treeStringRequest) {
    // TODO: Implement tree string generation for the schema.
    throw Status.UNIMPLEMENTED.withDescription("TreeString not implemented").asRuntimeException();
  }

  /**
   * Handles the IS_LOCAL analysis request.
   *
   * <p>This operation determines if the given plan can be executed locally on the driver. In a Beam
   * context, this would likely always be false.
   */
  private AnalyzePlanResponse.IsLocal handleIsLocal(AnalyzePlanRequest.IsLocal isLocalRequest) {
    return AnalyzePlanResponse.IsLocal.newBuilder().setIsLocal(false).build();
  }

  /**
   * Handles the IS_STREAMING analysis request.
   *
   * <p>This operation determines if the given plan is a streaming plan. In Beam, this can be
   * determined by checking the boundedness of the PCollections.
   */
  private AnalyzePlanResponse.IsStreaming handleIsStreaming(
      AnalyzePlanRequest.IsStreaming isStreamingRequest) {
    // TODO: Implement by checking the boundedness of the input PCollections in the plan.
    throw Status.UNIMPLEMENTED.withDescription("IsStreaming not implemented").asRuntimeException();
  }

  /**
   * Handles the INPUT_FILES analysis request.
   *
   * <p>This operation returns a list of input files for the given plan. This is most relevant for
   * file-based data sources.
   */
  private AnalyzePlanResponse.InputFiles handleInputFiles(
      AnalyzePlanRequest.InputFiles inputFilesRequest) {
    // TODO: This would require inspecting the source transforms in the Beam pipeline.
    throw Status.UNIMPLEMENTED.withDescription("InputFiles not implemented").asRuntimeException();
  }

  /**
   * Handles the SPARK_VERSION analysis request.
   *
   * <p>This operation returns the version of the Spark Connect server.
   */
  private AnalyzePlanResponse.SparkVersion handleSparkVersion(
      AnalyzePlanRequest.SparkVersion sparkVersionRequest) {
    // You can return a hardcoded version string for your custom server.
    return AnalyzePlanResponse.SparkVersion.newBuilder().setVersion("1.0.0-custom-beam").build();
  }

  /**
   * Handles the DDL_PARSE analysis request.
   *
   * <p>This operation parses a DDL string (e.g., "a INT, b STRING") into a Spark `DataType`.
   */
  private AnalyzePlanResponse.DDLParse handleDdlParse(AnalyzePlanRequest.DDLParse ddlParseRequest) {
    // TODO: Implement DDL parsing. You might be able to use Calcite's parser for this.
    throw Status.UNIMPLEMENTED.withDescription("DDLParse not implemented").asRuntimeException();
  }

  /**
   * Handles the SAME_SEMANTICS analysis request.
   *
   * <p>This operation checks if two logical plans are semantically equivalent.
   */
  private AnalyzePlanResponse.SameSemantics handleSameSemantics(
      AnalyzePlanRequest.SameSemantics sameSemanticsRequest) {
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
      AnalyzePlanRequest.SemanticHash semanticHashRequest) {
    // TODO: Implement semantic hashing for plans.
    throw Status.UNIMPLEMENTED.withDescription("SemanticHash not implemented").asRuntimeException();
  }

  /**
   * Handles the PERSIST analysis request.
   *
   * <p>This operation is a request to cache a DataFrame.
   */
  private AnalyzePlanResponse.Persist handlePersist(AnalyzePlanRequest.Persist persistRequest) {
    // TODO: Implement caching. This would involve materializing the PCollection.
    throw Status.UNIMPLEMENTED.withDescription("Persist not implemented").asRuntimeException();
  }

  /**
   * Handles the UNPERSIST analysis request.
   *
   * <p>This operation is a request to uncache a DataFrame.
   */
  private AnalyzePlanResponse.Unpersist handleUnpersist(
      AnalyzePlanRequest.Unpersist unpersistRequest) {
    // TODO: Implement uncaching.
    throw Status.UNIMPLEMENTED.withDescription("Unpersist not implemented").asRuntimeException();
  }

  /**
   * Handles the GET_STORAGE_LEVEL analysis request.
   *
   * <p>This operation retrieves the storage level of a cached DataFrame.
   */
  private AnalyzePlanResponse.GetStorageLevel handleGetStorageLevel(
      AnalyzePlanRequest.GetStorageLevel getStorageLevelRequest) {
    // TODO: Implement storage level retrieval.
    throw Status.UNIMPLEMENTED
        .withDescription("GetStorageLevel not implemented")
        .asRuntimeException();
  }

  /**
   * Handles the JSON_TO_DDL analysis request.
   *
   * <p>This operation converts a JSON schema string to a DDL string.
   */
  private AnalyzePlanResponse.JsonToDDL handleJsonToDdl(
      AnalyzePlanRequest.JsonToDDL jsonToDdlRequest) {
    // TODO: Implement JSON schema to DDL conversion.
    throw Status.UNIMPLEMENTED.withDescription("JsonToDDL not implemented").asRuntimeException();
  }
}
