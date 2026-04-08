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

import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.csv.CsvIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sparkconnect.ProtoUtils;
import org.apache.beam.sparkconnect.RowToArrowConverter;
import org.apache.beam.sparkconnect.SparkRelationToRelNode;
import org.apache.beam.sparkconnect.rel.SparkLocalRelation;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexUtil;
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
  private final Map<String, String> conf;

  public ExecutePlanHandler(
      Map<String, List<ExecutePlanResponse>> operationToResponses,
      BeamSqlEnv beamSqlEnv,
      Map<String, String> conf) {
    this.operationToResponses = operationToResponses;
    this.beamSqlEnv = beamSqlEnv;
    this.conf = conf;
  }

  public void handle(
      ExecutePlanRequest request, StreamObserver<ExecutePlanResponse> responseObserver) {
    LOG.debug("executePlan request:\n{}", ProtoUtils.debugString(request));

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
          handleRootPlan(request.getPlan().getRoot(), responseBuilder);
          break;
        case COMMAND:
          handleCommand(request.getPlan().getCommand(), responseBuilder);
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
    } catch (Exception exc) {
      LOG.error("Error handling executePlan", exc);
      responseObserver.onError(exc);
    }
  }

  private void handleCommand(Command command, ExecutePlanResponse.Builder responseBuilder)
      throws IOException {
    switch (command.getCommandTypeCase()) {
      case SQL_COMMAND:
        handleSqlCommand(command.getSqlCommand(), responseBuilder);
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

      case ML_COMMAND:
        // PySpark attempts to clean up the ML cache after test execution.
        // We just ignore this command as we don't have ML cache.
        LOG.debug("Ignoring ML_COMMAND");
        break;

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
  private void handleSqlCommand(SqlCommand sqlCommand, ExecutePlanResponse.Builder responseBuilder)
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

    // --- Preprocess SQL to handle Spark-specific syntax that Calcite doesn't like ---
    // Handle: SELECT * FROM VALUES (...) AS tab(...) -> SELECT * FROM (VALUES (...)) AS tab(...)
    if (sql.toUpperCase().contains("FROM VALUES") && sql.toUpperCase().contains(" AS ")) {
      sql = sql.replaceAll("(?i)FROM\\s+VALUES\\b([\\s\\S]*?)\\bAS\\b", "FROM (VALUES $1) AS");
    }

    // Standardize Spark literal constructors to Calcite syntax
    sql = sql.replaceAll("(?i)\\bDATE\\s*\\(\\s*'([^']+)'\\s*\\)", "DATE '$1'");
    sql = sql.replaceAll("(?i)\\bTIMESTAMP\\s*\\(\\s*'([^']+)'\\s*\\)", "TIMESTAMP '$1'");

    // Handle RANGE(N) table generating function by converting it to a VALUES clause for Calcite
    java.util.regex.Pattern rangePattern =
        java.util.regex.Pattern.compile("(?i)\\bFROM\\s+RANGE\\s*\\(\\s*(\\d+)\\s*\\)");
    java.util.regex.Matcher rangeMatcher = rangePattern.matcher(sql);
    if (rangeMatcher.find()) {
      try {
        String g1 = rangeMatcher.group(1);
        if (g1 != null) {
          int n = Integer.parseInt(g1);
          if (n <= 1000) { // Safety limit for query size
            StringBuilder valuesBuilder = new StringBuilder();
            valuesBuilder.append("FROM (VALUES ");
            for (int i = 0; i < n; i++) {
              valuesBuilder.append("ROW(").append(i).append(")");
              if (i < n - 1) {
                valuesBuilder.append(", ");
              }
            }
            valuesBuilder.append(") AS tab(id)");
            sql = rangeMatcher.replaceAll(valuesBuilder.toString());
          }
        }
      } catch (NumberFormatException e) {
        // Fall back if number is too large or invalid
      }
    }
    // Standardize Spark types to Calcite types
    sql =
        sql.replaceAll(
            "(?i)\\bCAST\\s*\\(\\s*([^)]+)\\s+AS\\s+STRING\\s*\\)", "CAST($1 AS VARCHAR)");

    // Handle STRUCT(...) row construction by converting it to ROW(...)
    sql = sql.replaceAll("(?i)\\bSTRUCT\\s*\\(", "ROW(");

    // Handle float(...) and double(...) simple type constructors in VALUES
    sql = sql.replaceAll("(?i)\\bfloat\\s*\\(\\s*([^)]+?)\\s*\\)", "CAST($1 AS FLOAT)");
    sql = sql.replaceAll("(?i)\\bdouble\\s*\\(\\s*([^)]+?)\\s*\\)", "CAST($1 AS DOUBLE)");

    // Handle MAP(...) and ARRAY(...) constructors by converting them to MAP[...] and ARRAY[...] for
    // Calcite
    sql = sql.replaceAll("(?i)\\bMAP\\s*\\(\\s*([^)]+?)\\s*\\)", "MAP[$1]");
    sql = sql.replaceAll("(?i)\\bARRAY\\s*\\(\\s*([^)]+?)\\s*\\)", "ARRAY[$1]");

    if (beamSqlEnv.isDdl(sql)) {
      beamSqlEnv.executeDdl(sql);
    } else {
      // Use the BeamSqlEnv to parse and plan the SQL query.
      // This will return a BeamRelNode ready for execution.
      BeamRelNode beamRelNode = beamSqlEnv.parseQuery(sql);

      // Reuse the existing execution logic to run the plan and send the results.
      executeCalcitePlanAndRespond(beamRelNode, responseBuilder);
    }
  }

  private void handleRootPlan(Relation root, ExecutePlanResponse.Builder responseBuilder)
      throws IOException {

    SparkRelationToRelNode sparkRelationToRelNode = new SparkRelationToRelNode(beamSqlEnv, conf);
    RelNode relNode = sparkRelationToRelNode.translate(root);

    if (relNode instanceof SparkLocalRelation) {
      SparkLocalRelation localRel = (SparkLocalRelation) relNode;
      respondWithArrow(
          localRel.getRows(),
          CalciteUtils.toSchema(localRel.deriveRowType()),
          java.util.Collections.emptySet(),
          responseBuilder);
    } else if (relNode instanceof LogicalProject) {
      LogicalProject project = (LogicalProject) relNode;
      if (project.getInput() instanceof SparkLocalRelation
          && RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
        SparkLocalRelation localRel = (SparkLocalRelation) project.getInput();
        respondWithArrow(
            localRel.getRows(),
            CalciteUtils.toSchema(localRel.deriveRowType()),
            java.util.Collections.emptySet(),
            responseBuilder);
      } else {
        executeCalcitePlanAndRespond(beamSqlEnv.convertToBeamRel(relNode), responseBuilder);
      }
    } else {
      executeCalcitePlanAndRespond(beamSqlEnv.convertToBeamRel(relNode), responseBuilder);
    }
  }

  private void executeCalcitePlanAndRespond(
      BeamRelNode beamRelNode, ExecutePlanResponse.Builder responseBuilder) throws IOException {

    List<Row> outputRows = BeamEnumerableConverter.toRowList(beamRelNode);

    org.apache.beam.sdk.schemas.Schema beamSchema =
        org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.toSchema(
            beamRelNode.getRowType());

    java.util.Set<String> nullFields = new java.util.HashSet<>();
    for (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField field :
        beamRelNode.getRowType().getFieldList()) {
      if (field.getType().getSqlTypeName()
          == org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName.NULL) {
        nullFields.add(field.getName());
      }
    }

    java.util.Set<Integer> ntzFieldIndices = new java.util.HashSet<>();
    if (beamRelNode
        instanceof org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Calc) {
      org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Calc calc =
          (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Calc) beamRelNode;
      org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexProgram program =
          calc.getProgram();
      List<org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLocalRef> projects =
          program.getProjectList();
      for (int i = 0; i < projects.size(); i++) {
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLocalRef ref =
            projects.get(i);
        org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode expr =
            program.getExprList().get(ref.getIndex());
        if (expr
            instanceof org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef) {
          org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef inputRef =
              (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef) expr;
          int inputIndex = inputRef.getIndex();
          org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType
              inputRowType = calc.getInput().getRowType();
          String inputFieldName = inputRowType.getFieldNames().get(inputIndex);
          if (inputFieldName.endsWith("__ntz")) {
            ntzFieldIndices.add(i);
          }
        }
      }
    }

    org.apache.beam.sdk.schemas.Schema.Builder schemaBuilder =
        org.apache.beam.sdk.schemas.Schema.builder();
    List<org.apache.beam.sdk.schemas.Schema.Field> fields = beamSchema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      org.apache.beam.sdk.schemas.Schema.Field field = fields.get(i);
      String fieldName = field.getName();
      if (ntzFieldIndices.contains(i) || fieldName.endsWith("__ntz")) {
        String cleanName =
            fieldName.endsWith("__ntz")
                ? fieldName.substring(0, fieldName.length() - 5)
                : fieldName;
        schemaBuilder.addField(
            org.apache.beam.sdk.schemas.Schema.Field.of(
                    cleanName, field.getType().withMetadata("spark_type", "timestamp_ntz"))
                .withNullable(field.getType().getNullable()));
      } else {
        schemaBuilder.addField(field);
      }
    }
    org.apache.beam.sdk.schemas.Schema newSchema = schemaBuilder.build();

    List<Row> newRows = new java.util.ArrayList<>();
    for (Row row : outputRows) {
      newRows.add(Row.withSchema(newSchema).addValues(row.getValues()).build());
    }
    outputRows = newRows;
    beamSchema = newSchema;

    respondWithArrow(outputRows, beamSchema, nullFields, responseBuilder);
  }

  private void respondWithArrow(
      List<Row> outputRows,
      org.apache.beam.sdk.schemas.Schema beamSchema,
      java.util.Set<String> nullFields,
      ExecutePlanResponse.Builder responseBuilder)
      throws IOException {

    Schema arrowSchema = RowToArrowConverter.toArrowSchema(beamSchema, nullFields);

    ExecutePlanResponse.ArrowBatch.Builder arrowBatchBuilder =
        ExecutePlanResponse.ArrowBatch.newBuilder();
    arrowBatchBuilder.setRowCount(outputRows.size());

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      try (VectorSchemaRoot arrowRoot = VectorSchemaRoot.create(arrowSchema, allocator)) {
        RowToArrowConverter.populateVectorSchemaRoot(arrowRoot, outputRows, beamSchema);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = newArrowStreamWriter(arrowRoot, null, out)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }

        arrowBatchBuilder.setData(com.google.protobuf.ByteString.copyFrom(out.toByteArray()));
      }
    }

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
    SparkRelationToRelNode translator = new SparkRelationToRelNode(beamSqlEnv, conf);
    RelNode relNode = translator.translate(writeOperation.getInput());
    BeamRelNode beamRelNode = beamSqlEnv.convertToBeamRel(relNode);

    PipelineOptions options =
        BeamEnumerableConverter.createPipelineOptions(beamSqlEnv.getPipelineOptions());
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
        CSVFormat csvFormat =
            createCsvFormat(writeOperation.getOptionsMap(), inputPCollection.getSchema());
        CsvIO.Write<Row> writeRows = CsvIO.writeRows(path + "part", csvFormat).withSuffix(".csv");
        inputPCollection.apply("WriteCSV", writeRows);
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
  public static CSVFormat createCsvFormat(
      Map<String, String> options, org.apache.beam.sdk.schemas.Schema schema) {

    // Rather than use what commons considers "default" we should set defaults to match Spark
    CSVFormat format = CSVFormat.DEFAULT;

    // Set the header from the schema if the 'header' option is true.
    if (Boolean.parseBoolean(options.getOrDefault("header", "false"))) {
      format = format.withHeader(schema.getFieldNames().toArray(new String[0]));
    }

    // Delimiter (sep)
    String sep = options.get("sep");
    if (sep != null && !sep.isEmpty()) {
      format = format.withDelimiter(sep.charAt(0));
    }

    // Quote character
    String quote = options.get("quote");
    if (quote != null && !quote.isEmpty()) {
      format = format.withQuote(quote.charAt(0));
    }

    // Escape character
    String escape = options.get("escape");
    if (escape != null && !escape.isEmpty()) {
      format = format.withEscape(escape.charAt(0));
    }

    // Null value representation
    String nullValue = options.get("nullValue");
    if (nullValue != null && !nullValue.isEmpty()) {
      format = format.withNullString(nullValue);
    }

    // Quote mode
    if (Boolean.parseBoolean(options.getOrDefault("quoteAll", "false"))) {
      format = format.withQuoteMode(QuoteMode.ALL);
    }

    // Line separator is permissive in read and defaults to \n in writes
    String lineSep = options.get("lineSep");
    if (lineSep != null && !lineSep.isEmpty()) {
      format = format.withRecordSeparator(lineSep);
    } else {
      format = format.withRecordSeparator('\n');
    }

    // NOTE: There are many more options particularly around dateFormat and timestampFormat
    // that we'd add a DoFn for

    return format;
  }

  /** Maps Spark's compression codec names to Beam's {@link Compression} enum. */
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
  private void handleTableWrite(@SuppressWarnings("unused") WriteOperation writeOperation) {
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
