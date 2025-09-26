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
import org.apache.spark.connect.proto.ReattachExecuteRequest;
import org.apache.spark.connect.proto.ReleaseExecuteRequest;
import org.apache.spark.connect.proto.ReleaseExecuteResponse;
import org.apache.spark.connect.proto.ReleaseSessionRequest;
import org.apache.spark.connect.proto.ReleaseSessionResponse;
import org.apache.spark.connect.proto.SparkConnectServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkConnectService extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(SparkConnectService.class);

  /**
   * Executes a request that contains the query and returns a stream of [[Response]]. It is
   * guaranteed that there is at least one ARROW batch returned even if the result set is empty.
   */
  @Override
  public void executePlan(
      ExecutePlanRequest request, StreamObserver<ExecutePlanResponse> responseObserver) {
    LOG.info("executePlan request");
    super.executePlan(request, responseObserver);
  }

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
    LOG.info("config request");
    responseObserver.onNext(
    super.config(request, responseObserver);
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
    LOG.info("interrupt request");
    super.interrupt(request, responseObserver);
  }

  /**
   * Reattach to an existing reattachable execution. The ExecutePlan must have been started with
   * ReattachOptions.reattachable=true. If the ExecutePlanResponse stream ends without a
   * ResultComplete message, there is more to continue.
   */
  @Override
  public void reattachExecute(
      ReattachExecuteRequest request, StreamObserver<ExecutePlanResponse> responseObserver) {
    LOG.info("reattachExecute request");
    super.reattachExecute(request, responseObserver);
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
    LOG.info("releaseExecute request");
    super.releaseExecute(request, responseObserver);
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
