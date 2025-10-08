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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.spark.connect.proto.AddArtifactsRequest;
import org.apache.spark.connect.proto.AddArtifactsResponse;
import org.apache.spark.connect.proto.AnalyzePlanRequest;
import org.apache.spark.connect.proto.AnalyzePlanResponse;
import org.apache.spark.connect.proto.ArtifactStatusesRequest;
import org.apache.spark.connect.proto.ArtifactStatusesResponse;
import org.apache.spark.connect.proto.ConfigRequest;
import org.apache.spark.connect.proto.ConfigResponse;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.apache.spark.connect.proto.FetchErrorDetailsRequest;
import org.apache.spark.connect.proto.FetchErrorDetailsResponse;
import org.apache.spark.connect.proto.InterruptRequest;
import org.apache.spark.connect.proto.InterruptResponse;
import org.apache.spark.connect.proto.KeyValue;
import org.apache.spark.connect.proto.ReattachExecuteRequest;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.ReleaseExecuteRequest;
import org.apache.spark.connect.proto.ReleaseExecuteResponse;
import org.apache.spark.connect.proto.ReleaseSessionRequest;
import org.apache.spark.connect.proto.ReleaseSessionResponse;
import org.apache.spark.connect.proto.SparkConnectServiceGrpc;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkConnectService extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(SparkConnectService.class);

  // This will need to be a per-session conf
  private final Map<String, String> conf = new HashMap<>();

  SparkConnectService() {
    conf.put("spark.sql.session.localRelationCacheThreshold", "" + (64 * 1024 * 1024));
    conf.put("spark.sql.execution.arrow.useLargeVarTypes", "false");
  }

  // HACK: map OperationId to the fake response(s) for it
  private final Map<String, List<ExecutePlanResponse>> operationToExecutePlanResponse =
      new HashMap<>();

  /**
   * Executes a request that contains the query and returns a stream of [[Response]]. It is
   * guaranteed that there is at least one ARROW batch returned even if the result set is empty.
   */
  @Override
  public void executePlan(
      ExecutePlanRequest request, StreamObserver<ExecutePlanResponse> responseObserver) {
    LOG.info("executePlan request {}", request);

    ExecutePlanResponse.Builder responseBuilder =
        ExecutePlanResponse.newBuilder()
            .setSessionId(request.getSessionId())
            .setResponseId(UUID.randomUUID().toString())
            .setOperationId(
                request
                    .getOperationId()); // this can be set by the server if the client didn't set it

    try {
      switch (request.getPlan().getOpTypeCase()) {
        case ROOT:
          handleRootPlan(request.getPlan().getRoot(), responseBuilder, responseObserver);
          break;
        case COMMAND:
          throw new UnsupportedOperationException("OpType COMMAND not yet supported");
        case OPTYPE_NOT_SET:
          throw new IllegalArgumentException("OpType not set");
        default:
          throw new UnsupportedOperationException(
              "Unrecognized OpType for plan: " + request.getPlan().getOpTypeCase().name());
      }

      operationToExecutePlanResponse.put(
          request.getOperationId(), ImmutableList.of(responseBuilder.build()));

      sendResponses(request.getOperationId(), responseBuilder, responseObserver, null);
    } catch (IOException exc) {
      responseObserver.onError(exc);
      responseObserver.onNext(
          responseBuilder
              .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
              .build());
      responseObserver.onCompleted();
    }
  }

  private void sendResponses(
      String operationId,
      ExecutePlanResponse.Builder responseBuilder,
      StreamObserver<ExecutePlanResponse> responseObserver,
      @Nullable String lastResponseId) {
    List<ExecutePlanResponse> responses = operationToExecutePlanResponse.get(operationId);

    if (responses == null) {
      throw new IllegalArgumentException("operation not found: " + operationId);
    }

    boolean shouldSend = lastResponseId == null;
    for (ExecutePlanResponse response : responses) {
      if (shouldSend) {
        responseObserver.onNext(response);
      } else {
        if (response.getResponseId().equals(lastResponseId)) {
          shouldSend = true;
        }
      }
    }
    responseObserver.onNext(
        responseBuilder
            .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
            .build());
    responseObserver.onCompleted();
  }

  private void handleRootPlan(
      Relation root,
      ExecutePlanResponse.Builder responseBuilder,
      StreamObserver<ExecutePlanResponse> responseObserver)
      throws IOException {

    // Run through the paces to check that it doesn't crash (yet)
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlEnv.BeamSqlEnvBuilder sqlEnvBuilder = BeamSqlEnv.builder(catalogManager);
    sqlEnvBuilder.setQueryPlannerClassName(CalciteQueryPlanner.class.getCanonicalName());
    PipelineOptions options = PipelineOptionsFactory.create();
    sqlEnvBuilder.setPipelineOptions(options);
    BeamSqlEnv sqlEnv = sqlEnvBuilder.build();
    RelBuilder relBuilder = sqlEnv.getRelBuilder();
    RelNode relNode = RelationToCalcite.translateRelationToRel(root, relBuilder);
    sqlEnv.convertToBeamRel(relNode);

    executeCalcitePlanAndRespond(relNode, responseBuilder, responseObserver);
  }

  private void executeCalcitePlanAndRespond(
      RelNode relNode,
      ExecutePlanResponse.Builder responseBuilder,
      StreamObserver<ExecutePlanResponse> responseObserver)
      throws IOException {

    // ignore the relNode and build a ShowString response

    // fake arrow batch that is a response for show_string
    ExecutePlanResponse.ArrowBatch.Builder arrowBatchBuilder =
        ExecutePlanResponse.ArrowBatch.newBuilder();
    arrowBatchBuilder.setRowCount(1);

    // this goes into the ShowString relation translator
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field showStringField =
          new Field(
              "show_string", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
      Schema schema = new Schema(Collections.singletonList(showStringField));

      // 2. Create VectorSchemaRoot
      try (VectorSchemaRoot arrowRoot = VectorSchemaRoot.create(schema, allocator)) {

        // Get the vector from the root
        VarCharVector varCharVector = (VarCharVector) arrowRoot.getVector("show_string");

        // 3. Allocate and Populate the Vector
        varCharVector.allocateNew(1);
        varCharVector.setSafe(0, "test test test".getBytes(StandardCharsets.UTF_8));
        varCharVector.setValueCount(1);
        arrowRoot.setRowCount(1);

        // 4. Serialize the VectorSchemaRoot to byte array
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = newArrowStreamWriter(arrowRoot, null, out)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        } // writer is closed here

        arrowBatchBuilder.setData(ByteString.copyFrom(out.toByteArray()));
      } // arrowRoot closed here
    } // allocator closed here

    responseBuilder.setArrowBatch(arrowBatchBuilder.build());
  }

  /** Wrapper for non-annotated Arrow library call that accepts nulls. */
  @SuppressWarnings("nullness")
  private ArrowStreamWriter newArrowStreamWriter(
      VectorSchemaRoot arrowRoot,
      @Nullable DictionaryProvider provider,
      OutputStream outputstream) {
    return new ArrowStreamWriter(arrowRoot, provider, outputstream);
  }

  // directly lifted - can't share because private
  //  private DataType parseDatatypeString(String sqlText) {
  //    DataTypeParserInterface parser = Parsers
  //    try {
  //      return parser.parseTableSchema(sqlText)
  //    } catch(ParseException e) {
  //        try {
  //          parser.parseDataType(sqlText);
  //        } catch {
  //        case _: ParseException =>
  //          try {
  //            parser.parseDataType(s"struct<${sqlText.trim}>")
  //          } catch {
  //          case _: ParseException =>
  //            throw e/
  //        }
  //      }
  //    }
  //

  /** Analyzes a query and returns a [[AnalyzeResponse]] containing metadata about the query. */
  @Override
  public void analyzePlan(
      AnalyzePlanRequest request, StreamObserver<AnalyzePlanResponse> responseObserver) {
    LOG.info("analyzePlan request");
    super.analyzePlan(request, responseObserver);
  }

  /** Update or fetch the configurations and returns a [[ConfigResponse]] containing the result. */
  @Override
  public void config(ConfigRequest request, StreamObserver<ConfigResponse> responseObserver) {
    LOG.info("config request: {}", request);

    ConfigResponse.Builder responseBuilder = ConfigResponse.newBuilder();

    responseBuilder.setSessionId(request.getSessionId());
    responseBuilder.setServerSideSessionId(request.getSessionId());
    responseBuilder.addWarnings("This is fake");

    // TBD which config options we actually need or want to support; for now we pretend!
    switch (request.getOperation().getOpTypeCase()) {
      case SET:
        break;
      case GET:
        handleConfigGet(request.getOperation().getGet(), responseBuilder);
        break;
      case GET_WITH_DEFAULT:
        break;
      case GET_OPTION:
        break;
      case GET_ALL:
        break;
      case UNSET:
        break;
      case IS_MODIFIABLE:
        for (String key : request.getOperation().getIsModifiable().getKeysList()) {
          responseBuilder.addPairs(KeyValue.newBuilder().setKey(key).setValue("false"));
        }
        break;
      case OPTYPE_NOT_SET:
        break;
    }

    LOG.info("config response: {}", responseBuilder);
    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }

  private void handleConfigGet(ConfigRequest.Get request, ConfigResponse.Builder responseBuilder) {
    for (String key : request.getKeysList()) {
      KeyValue.Builder kvBuilder = KeyValue.newBuilder();
      kvBuilder.setKey(key);
      @Nullable String value = conf.get(key);
      if (value != null) {
        kvBuilder.setValue(value);
      }
      responseBuilder.addPairs(kvBuilder.build());
    }
  }

  /**
   * Add artifacts to the session and returns a [[AddArtifactsResponse]] containing metadata about
   * the added artifacts.
   */
  @Override
  public StreamObserver<AddArtifactsRequest> addArtifacts(
      StreamObserver<AddArtifactsResponse> responseObserver) {
    LOG.info("addArtifact request");
    return super.addArtifacts(responseObserver);
  }

  /**
   *
   *
   * <pre>
   * Check statuses of artifacts in the session and returns them in a [[ArtifactStatusesResponse]].
   * </pre>
   */
  @Override
  public void artifactStatus(
      ArtifactStatusesRequest request, StreamObserver<ArtifactStatusesResponse> responseObserver) {
    LOG.info("artifactStatus request");
    super.artifactStatus(request, responseObserver);
  }

  /** Interrupts running executions. */
  @Override
  public void interrupt(
      InterruptRequest request,
      StreamObserver<org.apache.spark.connect.proto.InterruptResponse> responseObserver) {
    LOG.info("interrupt request {}", request);

    InterruptResponse.Builder responseBuilder = InterruptResponse.newBuilder();
    responseBuilder
        .setSessionId(request.getSessionId())
        .setServerSideSessionId(request.getSessionId());

    switch (request.getInterruptType()) {
      case INTERRUPT_TYPE_UNSPECIFIED:
        throw new IllegalArgumentException("interrupt type unspecified");
      case INTERRUPT_TYPE_ALL:
        // TODO
        // For now we pretend every operation that has been started has been interrupted
        responseBuilder.addAllInterruptedIds(operationToExecutePlanResponse.keySet());
        operationToExecutePlanResponse.clear();
        break;
      case INTERRUPT_TYPE_TAG:
        // TODO
        // These aren't specified in the proto so they have some other significance
        break;
      case INTERRUPT_TYPE_OPERATION_ID:
        // TODO
        // For now we pretend it was interrupted, and clear the response from the map
        responseBuilder.addInterruptedIds(request.getOperationId());
        operationToExecutePlanResponse.remove(request.getOperationId());
        break;
      case UNRECOGNIZED:
      default:
        throw new UnsupportedOperationException(
            "Unrecognized interrupt type: " + request.getInterruptType());
    }

    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }

  /**
   * Reattach to an existing reattachable execution. The ExecutePlan must have been started with
   * ReattachOptions.reattachable=true. If the ExecutePlanResponse stream ends without a
   * ResultComplete message, there is more to continue.
   */
  @Override
  public void reattachExecute(
      ReattachExecuteRequest request, StreamObserver<ExecutePlanResponse> responseObserver) {
    LOG.info("reattachExecute request {}", request);
    ExecutePlanResponse.Builder responseBuilder =
        ExecutePlanResponse.newBuilder()
            .setResponseId(UUID.randomUUID().toString())
            .setOperationId(request.getOperationId())
            .setSessionId(request.getSessionId())
            .setServerSideSessionId(request.getSessionId());

    sendResponses(
        request.getOperationId(), responseBuilder, responseObserver, request.getLastResponseId());
  }

  /**
   *
   *
   * <pre>
   * Release an reattachable execution, or parts thereof.
   * The ExecutePlan must have been started with ReattachOptions.reattachable=true.
   * Non reattachable executions are released automatically and immediately after the ExecutePlan
   * RPC and ReleaseExecute may not be used.
   * </pre>
   */
  @Override
  public void releaseExecute(
      ReleaseExecuteRequest request, StreamObserver<ReleaseExecuteResponse> responseObserver) {
    LOG.info("releaseExecute request {}", request);
    responseObserver.onNext(
        ReleaseExecuteResponse.newBuilder()
            .setSessionId(request.getSessionId())
            .setServerSideSessionId(request.getSessionId())
            .setOperationId(request.getOperationId())
            .build());
    responseObserver.onCompleted();
  }

  /**
   *
   *
   * <pre>
   * Release a session.
   * All the executions in the session will be released. Any further requests for the session with
   * that session_id for the given user_id will fail. If the session didn't exist or was already
   * released, this is a noop.
   * </pre>
   */
  @Override
  public void releaseSession(
      ReleaseSessionRequest request, StreamObserver<ReleaseSessionResponse> responseObserver) {
    LOG.info("releaseSession request");
    super.releaseSession(request, responseObserver);
  }

  /**
   *
   *
   * <pre>
   * FetchErrorDetails retrieves the matched exception with details based on a provided error id.
   * </pre>
   */
  @Override
  public void fetchErrorDetails(
      FetchErrorDetailsRequest request,
      StreamObserver<FetchErrorDetailsResponse> responseObserver) {
    LOG.info("fetchErrorDetails request");
    super.fetchErrorDetails(request, responseObserver);
  }
}
