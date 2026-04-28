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

import io.grpc.stub.StreamObserver;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sparkconnect.handler.AnalyzePlanHandler;
import org.apache.beam.sparkconnect.handler.ConfigHandler;
import org.apache.beam.sparkconnect.handler.ExecutePlanHandler;
import org.apache.beam.sparkconnect.rule.SparkConnectRuleSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSets;
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
import org.apache.spark.connect.proto.ReattachExecuteRequest;
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

  private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

  private Session getOrCreateSession(String sessionId) {
    return sessions.computeIfAbsent(sessionId, k -> new Session());
  }

  private static class Session {
    final Map<String, String> conf = new ConcurrentHashMap<>();
    final Map<String, List<ExecutePlanResponse>> operationToExecutePlanResponse =
        new ConcurrentHashMap<>();
    final Map<String, org.apache.spark.connect.proto.StorageLevel> relationStorageLevels =
        new ConcurrentHashMap<>();
    final BeamSqlEnv beamSqlEnv;

    Session() {
      conf.put("spark.sql.timestampType", "TIMESTAMP_LTZ");
      conf.put("spark.sql.session.timeZone", "UTC");
      conf.put("spark.sql.session.localRelationCacheThreshold", "" + (64 * 1024 * 1024));
      conf.put("spark.sql.session.localRelationSizeLimit", "" + (64 * 1024 * 1024));
      conf.put("spark.sql.session.localRelationChunkSizeRows", "10000");
      conf.put("spark.sql.session.localRelationChunkSizeBytes", "" + (16 * 1024 * 1024));
      conf.put("spark.sql.session.localRelationBatchOfChunksSizeBytes", "" + (128 * 1024 * 1024));
      conf.put("spark.sql.execution.pandas.convertToArrowArraySafely", "false");
      conf.put("spark.sql.execution.pandas.inferPandasDictAsMap", "false");
      conf.put("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false");
      conf.put("spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled", "false");
      conf.put("spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled", "false");
      conf.put("spark.sql.execution.arrow.useLargeVarTypes", "false");
      conf.put("spark.python.sql.dataFrameDebugging.enabled", "true");

      beamSqlEnv = getBeamSqlEnv();
    }
  }

  public static class SparkFunctions {
    public static @Nullable Timestamp timestamp(@Nullable String s) {
      return (s == null) ? null : Timestamp.valueOf(s);
    }

    public static Integer[] sequence(int start, int stop) {
      int size = Math.abs(stop - start) + 1;
      Integer[] arr = new Integer[size];
      int step = (start <= stop) ? 1 : -1;
      for (int i = 0; i < size; i++) {
        arr[i] = start + i * step;
      }
      return arr;
    }
  }

  /**
   * Executes a request that contains the query and returns a stream of [[Response]]. It is
   * guaranteed that there is at least one ARROW batch returned even if the result set is empty.
   */
  @Override
  public void executePlan(
      ExecutePlanRequest request, StreamObserver<ExecutePlanResponse> responseObserver) {
    LOG.debug("executePlan request:\n{}", request.toString());

    Session session = getOrCreateSession(request.getSessionId());
    new ExecutePlanHandler(session.operationToExecutePlanResponse, session.beamSqlEnv, session.conf)
        .handle(request, responseObserver);
  }

  private void sendResponses(
      Map<String, List<ExecutePlanResponse>> operationToResponses,
      String operationId,
      ExecutePlanResponse.Builder responseBuilder,
      StreamObserver<ExecutePlanResponse> responseObserver,
      @Nullable String lastResponseId) {
    List<ExecutePlanResponse> responses = operationToResponses.get(operationId);

    if (responses == null) {
      throw new IllegalArgumentException("operation not found: " + operationId);
    }

    // linear scan for the responses that should be sent
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

  private static BeamSqlEnv getBeamSqlEnv() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    catalogManager.registerTableProvider(new TextTableProvider());
    catalogManager.registerTableProvider(
        new org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider());
    BeamSqlEnv.BeamSqlEnvBuilder sqlEnvBuilder = BeamSqlEnv.builder(catalogManager);
    sqlEnvBuilder.setQueryPlannerClassName(CalciteQueryPlanner.class.getCanonicalName());

    // Register custom UDFs
    sqlEnvBuilder.addUdf("TIMESTAMP", SparkFunctions.class, "timestamp");
    sqlEnvBuilder.addUdf("SEQUENCE", SparkFunctions.class, "sequence");

    PipelineOptions options = PipelineOptionsFactory.fromArgs("--targetParallelism=1").create();
    sqlEnvBuilder.setPipelineOptions(options);

    // All the Beam rules and also the SparkConnect rules
    // ... this seems to only work right when they are put into a single RuleSet
    sqlEnvBuilder.setRuleSets(
        ImmutableList.of(
            RuleSets.ofList(
                ImmutableList.<RelOptRule>builder()
                    .addAll(BeamRuleSets.getAllRules())
                    .addAll(SparkConnectRuleSet.INSTANCE)
                    .build())));
    BeamSqlEnv sqlEnv = sqlEnvBuilder.build();
    return sqlEnv;
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
    LOG.debug("analyzePlan request:\n{}", request.toString());

    Session session = getOrCreateSession(request.getSessionId());
    new AnalyzePlanHandler(session.beamSqlEnv, session.conf, session.relationStorageLevels)
        .handle(request, responseObserver);
  }

  /** Update or fetch the configurations and returns a [[ConfigResponse]] containing the result. */
  @Override
  public void config(ConfigRequest request, StreamObserver<ConfigResponse> responseObserver) {
    LOG.debug("config request:\n{}", request.toString());
    Session session = getOrCreateSession(request.getSessionId());
    new ConfigHandler(session.conf).handle(request, responseObserver);
  }

  /**
   * Add artifacts to the session and returns a [[AddArtifactsResponse]] containing metadata about
   * the added artifacts.
   */
  @Override
  public StreamObserver<AddArtifactsRequest> addArtifacts(
      StreamObserver<AddArtifactsResponse> responseObserver) {
    LOG.debug("addArtifact request");
    return new org.apache.beam.sparkconnect.handler.AddArtifactsHandler().handle(responseObserver);
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
    LOG.debug("artifactStatus request:\n{}", ProtoUtils.debugString(request));
    new org.apache.beam.sparkconnect.handler.ArtifactStatusHandler()
        .handle(request, responseObserver);
  }

  /** Interrupts running executions. */
  @Override
  public void interrupt(
      InterruptRequest request,
      StreamObserver<org.apache.spark.connect.proto.InterruptResponse> responseObserver) {
    LOG.debug("interrupt request:\n{}", ProtoUtils.debugString(request));

    Session session = getOrCreateSession(request.getSessionId());

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
        responseBuilder.addAllInterruptedIds(session.operationToExecutePlanResponse.keySet());
        session.operationToExecutePlanResponse.clear();
        break;
      case INTERRUPT_TYPE_TAG:
        // TODO
        // These aren't specified in the proto so they have some other significance
        break;
      case INTERRUPT_TYPE_OPERATION_ID:
        // TODO
        // For now we pretend it was interrupted, and clear the response from the map
        responseBuilder.addInterruptedIds(request.getOperationId());
        session.operationToExecutePlanResponse.remove(request.getOperationId());
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
    LOG.debug("reattachExecute request:\n{}", request.toString());
    Session session = getOrCreateSession(request.getSessionId());
    ExecutePlanResponse.Builder responseBuilder =
        ExecutePlanResponse.newBuilder()
            .setResponseId(UUID.randomUUID().toString())
            .setOperationId(request.getOperationId())
            .setSessionId(request.getSessionId())
            .setServerSideSessionId(request.getSessionId());

    sendResponses(
        session.operationToExecutePlanResponse,
        request.getOperationId(),
        responseBuilder,
        responseObserver,
        request.getLastResponseId());
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
    LOG.debug("releaseExecute request:\n{}", request.toString());
    Session session = getOrCreateSession(request.getSessionId());
    session.operationToExecutePlanResponse.remove(request.getOperationId());

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
    LOG.debug("releaseSession request:\n{}", request.toString());
    sessions.remove(request.getSessionId());
    new org.apache.beam.sparkconnect.handler.ReleaseSessionHandler()
        .handle(request, responseObserver);
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
    LOG.debug("fetchErrorDetails request:\n{}", ProtoUtils.debugString(request));
    new org.apache.beam.sparkconnect.handler.FetchErrorDetailsHandler()
        .handle(request, responseObserver);
  }
}
