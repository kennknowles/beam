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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sparkconnect.SparkRelationToRelNode;
import org.apache.beam.sparkconnect.rule.SparkConnectRuleSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.spark.connect.proto.Command;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SqlCommand;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutePlanHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutePlanHandler.class);
  private final Map<String, List<ExecutePlanResponse>> operationToResponses;
  private final BeamSqlEnv beamSqlEnv;

  public ExecutePlanHandler(
      Map<String, List<ExecutePlanResponse>> operationToResponses, BeamSqlEnv beamSqlEnv) {
    this.operationToResponses = operationToResponses;
    this.beamSqlEnv = beamSqlEnv;
  }

  public void handle(
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
          handleCommand(request.getPlan().getCommand(), responseBuilder, responseObserver);
          break;
        case OPTYPE_NOT_SET:
          throw new IllegalArgumentException("OpType not set");
        default:
          throw new UnsupportedOperationException(
              "Unrecognized OpType for plan: " + request.getPlan().getOpTypeCase().name());
      }

      operationToResponses.put(request.getOperationId(), ImmutableList.of(responseBuilder.build()));

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onNext(
          responseBuilder
              .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
              .build());
      responseObserver.onCompleted();
    } catch (IOException exc) {
      responseObserver.onError(exc);
      responseObserver.onNext(
          responseBuilder
              .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
              .build());
      responseObserver.onCompleted();
    }
  }

  private void handleCommand(
      Command command,
      ExecutePlanResponse.Builder responseBuilder,
      StreamObserver<ExecutePlanResponse> responseObserver)
      throws IOException {
    switch (command.getCommandTypeCase()) {
      case SQL_COMMAND:
        handleSqlCommand(command.getSqlCommand(), responseBuilder, responseObserver);
        break;

      case WRITE_STREAM_OPERATION_START:
        // This command is used to write a streaming DataFrame to a sink.
        // It includes options like the mode (append, complete, update), trigger, and sink details.
        // To implement this, you would translate the input relation, and then apply a
        // PTransform that writes to the specified sink with the given triggering.
        throw new UnsupportedOperationException("WriteStreamOperation not yet implemented.");

      case CREATE_DATAFRAME_VIEW:
        // This command creates a temporary view from a DataFrame.
        // In a Beam/Calcite context, this would be equivalent to registering a PCollection
        // as a table in the catalog for the current session.
        throw new UnsupportedOperationException("CreateDataFrameViewCommand not yet implemented.");

      case WRITE_OPERATION:
        // This is for writing a batch DataFrame to a sink.
        // It includes options for format (e.g., "parquet", "json"), mode (overwrite, append),
        // partitioning, and other sink-related configurations.
        throw new UnsupportedOperationException("WriteOperation not yet implemented.");

      case REGISTER_FUNCTION:
        // This command is used to register a User Defined Function (UDF) for the session.
        // To implement this in Beam/Calcite, you would need to create a Calcite SqlFunction
        // and register it with the planner.
        throw new UnsupportedOperationException("RegisterFunction not yet implemented.");

      case EXTENSION:
        // A command for custom extensions that are not part of the core Spark Connect protocol.
        throw new UnsupportedOperationException("Extension commands not yet implemented.");

      default:
        throw new UnsupportedOperationException(
            "Unrecognized CommandType: " + command.getCommandTypeCase().name());
    }
  }

  /**
   * Handles the SQL_COMMAND.
   *
   * <p>This operation takes a SQL string, parses and plans it using the Beam SQL environment, and
   * then executes the resulting plan.
   */
  private void handleSqlCommand(
      SqlCommand sqlCommand,
      ExecutePlanResponse.Builder responseBuilder,
      StreamObserver<ExecutePlanResponse> responseObserver)
      throws IOException {
    // Args are for parameterized queries, e.g., SELECT * FROM T WHERE id = ?
    if (!sqlCommand.getArgsMap().isEmpty()
        || !sqlCommand.getNamedArgumentsMap().isEmpty()
        || !sqlCommand.getPosArgsList().isEmpty()) {
      // TODO: Implement support for parameterized queries. This would involve replacing
      // the named parameters in the SQL string with Calcite's dynamic parameters (`?`)
      // and passing the evaluated literal values to the planner.
      // Note also that these are deprecated and we need to check all the same fields in the
      // SQL relation proto that is embedded
      throw new UnsupportedOperationException("Parameterized SQL queries are not yet supported.");
    }

    String sql = sqlCommand.getInput().getSql().getQuery();
    if (sql.isEmpty()) {
      // deprecated path
      sql = sqlCommand.getSql();
      if (sql.isEmpty()) {
        throw new IllegalArgumentException("No SQL in SqlCommand...");
      }
    }

    try {
      // Use the BeamSqlEnv to parse and plan the SQL query.
      // This will return a BeamRelNode ready for execution.
      BeamRelNode beamRelNode = beamSqlEnv.parseQuery(sql);

      // Reuse the existing execution logic to run the plan and send the results.
      executeCalcitePlanAndRespond(beamRelNode, responseBuilder, responseObserver);
    } catch (SqlConversionException e) {
      throw new RuntimeException("Failed to parse or plan SQL query: " + sql, e);
    }
  }

  private void handleRootPlan(
      Relation root,
      ExecutePlanResponse.Builder responseBuilder,
      StreamObserver<ExecutePlanResponse> responseObserver)
      throws IOException {

    SparkRelationToRelNode sparkRelationToRelNode =
        new SparkRelationToRelNode(beamSqlEnv.getRelBuilder().getCluster());
    RelNode relNode = sparkRelationToRelNode.translate(root);
    BeamRelNode beamRelNode = beamSqlEnv.convertToBeamRel(relNode);

    executeCalcitePlanAndRespond(beamRelNode, responseBuilder, responseObserver);
  }

  private static BeamSqlEnv getBeamSqlEnv() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlEnv.BeamSqlEnvBuilder sqlEnvBuilder = BeamSqlEnv.builder(catalogManager);
    sqlEnvBuilder.setQueryPlannerClassName(CalciteQueryPlanner.class.getCanonicalName());
    PipelineOptions options = PipelineOptionsFactory.create();
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

  private void executeCalcitePlanAndRespond(
      BeamRelNode beamRelNode,
      ExecutePlanResponse.Builder responseBuilder,
      StreamObserver<ExecutePlanResponse> responseObserver)
      throws IOException {

    List<Row> outputRows = BeamEnumerableConverter.toRowList(beamRelNode);

    // TODO: convert Beam schema into Arrow schema for sending response rows as Arrow
    Field showStringField =
        new Field("show_string", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
    Schema schema = new Schema(Collections.singletonList(showStringField));

    ExecutePlanResponse.ArrowBatch.Builder arrowBatchBuilder =
        ExecutePlanResponse.ArrowBatch.newBuilder();
    arrowBatchBuilder.setRowCount(outputRows.size());

    // this goes into the ShowString relation translator
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      try (VectorSchemaRoot arrowRoot = VectorSchemaRoot.create(schema, allocator)) {

        // Get the vector from the root
        VarCharVector varCharVector = (VarCharVector) arrowRoot.getVector("show_string");

        varCharVector.allocateNew(outputRows.size());
        for (int i = 0; i < outputRows.size(); i++) {
          // TODO: don't assume show_string schema
          String showString = checkStateNotNull(outputRows.get(i).getString("show_string"));
          varCharVector.setSafe(i, showString.getBytes(StandardCharsets.UTF_8));
        }
        varCharVector.setValueCount(outputRows.size());
        arrowRoot.setRowCount(outputRows.size());

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
}
