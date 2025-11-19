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
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.csv.CsvIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sparkconnect.SparkRelationToRelNode;
import org.apache.beam.sparkconnect.rule.SparkConnectRuleSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.spark.connect.proto.Command;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SqlCommand;
import org.apache.spark.connect.proto.WriteOperation;
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
        handleWriteOperation(command.getWriteOperation());
        break;

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

    if (beamSqlEnv.isDdl(sql)) {
      beamSqlEnv.executeDdl(sql);
    } else {
      // Use the BeamSqlEnv to parse and plan the SQL query.
      // This will return a BeamRelNode ready for execution.
      BeamRelNode beamRelNode = beamSqlEnv.parseQuery(sql);

      // Reuse the existing execution logic to run the plan and send the results.
      executeCalcitePlanAndRespond(beamRelNode, responseBuilder, responseObserver);
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

  /**
   * Handles the WriteOperation command. This can be a file-based save (`df.write.save()`) or a
   * table-based save (`df.write.saveAsTable()`).
   */
  private void handleWriteOperation(WriteOperation writeOperation) throws IOException {
    switch (writeOperation.getSaveTypeCase()) {
      case PATH:
        handleFileWrite(writeOperation);
        break;
      case TABLE:
        handleTableWrite(writeOperation);
        break;
      default:
        throw new UnsupportedOperationException(
            "WriteOperation save type not supported: " + writeOperation.getSaveTypeCase());
    }
  }

  private void handleFileWrite(WriteOperation writeOperation) throws IOException {
    if (!writeOperation.hasPath()) {
      throw new IllegalArgumentException(
          "WriteOperation for file-based saves must specify a 'path'.");
    }

    // 1. Translate the input relation to a PCollection<Row>.
    SparkRelationToRelNode translator =
        new SparkRelationToRelNode(beamSqlEnv.getRelBuilder().getCluster());
    RelNode relNode = translator.translate(writeOperation.getInput());
    BeamRelNode beamRelNode = beamSqlEnv.convertToBeamRel(relNode);

    PipelineOptions options = BeamEnumerableConverter.createPipelineOptions(beamSqlEnv.getPipelineOptions());
    Pipeline pipeline = Pipeline.create(options);
    PCollection<Row> inputPCollection = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    // 2. Extract configuration from the proto.
    String path = writeOperation.getPath();
    String format = writeOperation.hasSource() ? writeOperation.getSource() : "parquet";
    // List<String> partitionCols = writeOperation.getPartitioningColumnsList(); not dealing with
    // this at the moment,
    // but dynamic destingations can express it
    // Map<String, String> options = writeOperation.getOptionsMap(); not sure which of these will
    // matter

    // 3. Select the appropriate Beam IO sink based on the format.
    switch (format.toLowerCase()) {
      case "csv":
        CSVFormat csvFormat = createCsvFormat(writeOperation.getOptionsMap(), inputPCollection.getSchema());
        CsvIO.Write<Row> writeRows = CsvIO.writeRows(path + "part", csvFormat).withSuffix(".csv");
        inputPCollection.apply(
            "WriteCSV", writeRows);
        break;

      case "parquet":
        org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(inputPCollection.getSchema());

        PCollection<GenericRecord> avroRecords =
            inputPCollection.apply(
                "ToAvroRecords",
                MapElements.into(TypeDescriptor.of(GenericRecord.class))
                    .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)));

        avroRecords.apply(
            "WriteParquet", FileIO.<GenericRecord>write().to(path).via(ParquetIO.sink(avroSchema)));
        break;

      default:
        throw new UnsupportedOperationException("Output format not supported: " + format);
    }

    // 4. Run the pipeline to execute the write.
    pipeline.run().waitUntilFinish();
  }

  /**
   * Creates a {@link CSVFormat} object based on the options provided by the Spark Connect client.
   */
  private CSVFormat createCsvFormat(Map<String, String> options, org.apache.beam.sdk.schemas.Schema schema) {
    CSVFormat format = CSVFormat.DEFAULT;

    // Set the header from the schema if the 'header' option is true.
    if (Boolean.parseBoolean(options.getOrDefault("header", "false"))) {
      format = format.withHeader(schema.getFieldNames().toArray(new String[0]));
    }

    // Delimiter (sep)
    if (options.containsKey("sep")) {
      format = format.withDelimiter(options.get("sep").charAt(0));
    }

    // Quote character
    if (options.containsKey("quote")) {
      format = format.withQuote(options.get("quote").charAt(0));
    }

    // Escape character
    if (options.containsKey("escape")) {
      format = format.withEscape(options.get("escape").charAt(0));
    }

    // Null value representation
    if (options.containsKey("nullValue")) {
      format = format.withNullString(options.get("nullValue"));
    }

    // Quote mode
    if (Boolean.parseBoolean(options.getOrDefault("quoteAll", "false"))) {
      format = format.withQuoteMode(QuoteMode.ALL);
    }

    // Line separator
    if (options.containsKey("lineSep")) {
      format = format.withRecordSeparator(options.get("lineSep"));
    }

    // NOTE: dateFormat and timestampFormat are not directly supported by CsvIO's automatic
    // row conversion. Handling these would require a custom DoFn to format the values before
    // writing, similar to the original RowToCsv implementation.

    return format;
  }

  /**
   * Maps Spark's compression codec names to Beam's {@link Compression} enum.
   */
  private Compression getBeamCompression(String sparkCompression) {
    if (sparkCompression == null) {
      return Compression.AUTO;
    }
    switch (sparkCompression.toLowerCase()) {
      case "none":
      case "uncompressed":
        return Compression.UNCOMPRESSED;
      case "gzip":
        return Compression.GZIP;
      case "bzip2":
        return Compression.BZIP2;
      case "deflate":
        return Compression.DEFLATE;
      // Add other mappings as needed (e.g., lz4, snappy)
      default:
        return Compression.AUTO;
    }
  }

  /** Handles writing a DataFrame to a table using `saveAsTable`. */
  private void handleTableWrite(WriteOperation writeOperation) {
    throw new UnsupportedOperationException("Not yet implemented");
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
