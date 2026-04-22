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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteTable;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogSchema;
import org.apache.beam.sdk.extensions.sql.impl.parser.SqlDdlNodes;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sparkconnect.rel.LogicalShowString;
import org.apache.beam.vendor.calcite.v1_40_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.arrow.ArrowFieldTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelCollations;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.AggregateCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalSort;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalValues;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlAggFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.ImmutableBitSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.spark.connect.proto.Aggregate;
import org.apache.spark.connect.proto.Deduplicate;
import org.apache.spark.connect.proto.Drop;
import org.apache.spark.connect.proto.Expression;
import org.apache.spark.connect.proto.Filter;
import org.apache.spark.connect.proto.Join;
import org.apache.spark.connect.proto.Limit;
import org.apache.spark.connect.proto.LocalRelation;
import org.apache.spark.connect.proto.Offset;
import org.apache.spark.connect.proto.Project;
import org.apache.spark.connect.proto.Range;
import org.apache.spark.connect.proto.Read;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SQL;
import org.apache.spark.connect.proto.SetOperation;
import org.apache.spark.connect.proto.ShowString;
import org.apache.spark.connect.proto.Sort;
import org.apache.spark.connect.proto.Tail;
import org.apache.spark.connect.proto.ToDF;
import org.apache.spark.connect.proto.WithColumns;
import org.apache.spark.connect.proto.WithColumnsRenamed;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkRelationToRelNode {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRelationToRelNode.class);

  private final RelOptCluster cluster;
  private final BeamSqlEnv beamSqlEnv;
  private final Map<String, String> conf;

  public SparkRelationToRelNode(BeamSqlEnv beamSqlEnv, Map<String, String> conf) {
    this.beamSqlEnv = beamSqlEnv;
    this.cluster = beamSqlEnv.getRelBuilder().getCluster();
    this.conf = conf;
  }

  // A map for common aggregate functions from Spark to Calcite.
  private static final Map<String, SqlAggFunction> AGG_OPERATOR_MAP =
      ImmutableMap.<String, SqlAggFunction>builder()
          .put("count", SqlStdOperatorTable.COUNT)
          .put("avg", SqlStdOperatorTable.AVG)
          .put("sum", SqlStdOperatorTable.SUM)
          .put("min", SqlStdOperatorTable.MIN)
          .put("max", SqlStdOperatorTable.MAX)
          .build();

  public RelNode translate(Relation sparkRelation) {
    switch (sparkRelation.getRelTypeCase()) {
      case READ:
        return translateRead(sparkRelation.getRead());
      case PROJECT:
        return translateProject(sparkRelation.getProject());
      case FILTER:
        return translateFilter(sparkRelation.getFilter());
      case JOIN:
        return translateJoin(sparkRelation.getJoin());
      case SET_OP:
        return translateSetOp(sparkRelation.getSetOp());
      case SORT:
        return translateSort(sparkRelation.getSort());
      case LIMIT:
        return translateLimit(sparkRelation.getLimit());
      case OFFSET:
        return translateOffset(sparkRelation.getOffset());
      case AGGREGATE:
        return translateAggregate(sparkRelation.getAggregate());
      case LOCAL_RELATION:
        return translateLocalRelation(sparkRelation.getLocalRelation());
      case DEDUPLICATE:
        return translateDeduplicate(sparkRelation.getDeduplicate());
      case RANGE:
        return translateRange(sparkRelation.getRange());
      case SQL:
        return translateSql(sparkRelation.getSql());
      case TAIL:
        return translateTail(sparkRelation.getTail());
      case DROP:
        return translateDrop(sparkRelation.getDrop());
      case WITH_COLUMNS_RENAMED:
        return translateWithColumnsRenamed(sparkRelation.getWithColumnsRenamed());
      case WITH_COLUMNS:
        return translateWithColumns(sparkRelation.getWithColumns());
      case TO_DF:
        return translateToDf(sparkRelation.getToDf());
      case SHOW_STRING:
        return translateShowString(sparkRelation.getShowString());
      default:
        return unsupported(sparkRelation.getRelTypeCase().name());
    }
  }

  private RelNode unsupported(String typeName) {
    throw new UnsupportedOperationException("Spark Relation type not supported yet: " + typeName);
  }

  private RelNode translateShowString(ShowString showStringProto) {
    RelNode input = translate(showStringProto.getInput());
    return new LogicalShowString(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        input,
        showStringProto.getNumRows(),
        showStringProto.getTruncate(),
        showStringProto.getVertical());
  }

  private RelNode translateProject(Project projectProto) {
    RelNode inputNode;
    if (projectProto.hasInput()) {
      inputNode = translate(projectProto.getInput());
    } else {
      inputNode = LogicalValues.createOneRow(cluster);
    }
    RelDataType inputRowType = inputNode.getRowType();

    SparkExpressionToRexNode exprConverter =
        new SparkExpressionToRexNode(cluster, inputRowType, beamSqlEnv.getOperatorTable());
    List<RexNode> calciteProjections =
        projectProto.getExpressionsList().stream()
            .map(exprConverter::translate)
            .collect(Collectors.toList());

    List<@Nullable String> fieldNames =
        exprConverter.deriveFieldNames(projectProto.getExpressionsList());

    return LogicalProject.create(
        inputNode, Collections.emptyList(), calciteProjections, fieldNames, Collections.emptySet());
  }

  private RelNode translateFilter(Filter filterProto) {
    RelNode inputNode = translate(filterProto.getInput());
    SparkExpressionToRexNode exprConverter =
        new SparkExpressionToRexNode(
            cluster, inputNode.getRowType(), beamSqlEnv.getOperatorTable());
    RexNode condition = exprConverter.translate(filterProto.getCondition());
    return LogicalFilter.create(inputNode, condition);
  }

  private RelNode translateJoin(Join joinProto) {
    RelNode left = translate(joinProto.getLeft());
    RelNode right = translate(joinProto.getRight());

    RexNode condition;
    int leftFieldCount = left.getRowType().getFieldCount();

    if (joinProto.hasJoinCondition()) {
      RelDataType joinRowType =
          cluster.getTypeFactory().createJoinType(left.getRowType(), right.getRowType());
      // Need an expression translateer that can handle field references from both left and right
      // inputs
      SparkExpressionToRexNode joinExprConverter =
          new SparkExpressionToRexNode(cluster, joinRowType, beamSqlEnv.getOperatorTable());
      condition = joinExprConverter.translate(joinProto.getJoinCondition());
    } else if (joinProto.getUsingColumnsCount() > 0) {
      List<RexNode> equiConditions = new ArrayList<>();
      for (String colName : joinProto.getUsingColumnsList()) {
        RelDataTypeField leftField = left.getRowType().getField(colName, false, false);
        RelDataTypeField rightField = right.getRowType().getField(colName, false, false);
        if (leftField == null)
          throw new IllegalArgumentException(
              "using_column " + colName + " not found in left join input");
        if (rightField == null)
          throw new IllegalArgumentException(
              "using_column " + colName + " not found in right join input");

        RexNode leftRef =
            cluster.getRexBuilder().makeInputRef(leftField.getType(), leftField.getIndex());
        RexNode rightRef =
            cluster
                .getRexBuilder()
                .makeInputRef(rightField.getType(), leftFieldCount + rightField.getIndex());
        equiConditions.add(
            cluster.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef));
      }
      // this might not be right - we should probably just build a RexNode matching the syntax and
      // save the join analysis for later
      condition = cluster.getRexBuilder().makeCall(SqlStdOperatorTable.AND, equiConditions);
    } else if (joinProto.getJoinType() == Join.JoinType.JOIN_TYPE_CROSS) {
      condition = cluster.getRexBuilder().makeLiteral(true);
    } else {
      throw new IllegalArgumentException(
          "Join must have a condition, using_columns, or be a CROSS join");
    }

    return LogicalJoin.create(
        left,
        right,
        ImmutableList.of(),
        condition,
        new HashSet<>(),
        translateSparkJoinType(joinProto.getJoinType()));
  }

  private JoinRelType translateSparkJoinType(Join.JoinType sparkJoinType) {
    switch (sparkJoinType) {
      case JOIN_TYPE_INNER:
        return JoinRelType.INNER;
      case JOIN_TYPE_LEFT_OUTER:
        return JoinRelType.LEFT;
      case JOIN_TYPE_RIGHT_OUTER:
        return JoinRelType.RIGHT;
      case JOIN_TYPE_FULL_OUTER:
        return JoinRelType.FULL;
      case JOIN_TYPE_LEFT_SEMI:
        return JoinRelType.SEMI;
      case JOIN_TYPE_LEFT_ANTI:
        return JoinRelType.ANTI;
      default:
        throw new UnsupportedOperationException("Spark JoinType not supported: " + sparkJoinType);
    }
  }

  private RelNode translateSetOp(SetOperation setOpProto) {
    RelNode left = translate(setOpProto.getLeftInput());
    RelNode right = translate(setOpProto.getRightInput());
    boolean all = setOpProto.getIsAll();
    // TODO: Handle by_name and allow_missing_columns for UNION
    switch (setOpProto.getSetOpType()) {
      case SET_OP_TYPE_UNION:
        return LogicalUnion.create(ImmutableList.of(left, right), all);
      case SET_OP_TYPE_INTERSECT:
        return LogicalIntersect.create(ImmutableList.of(left, right), all);
      case SET_OP_TYPE_EXCEPT:
        return LogicalMinus.create(ImmutableList.of(left, right), all);
      case SET_OP_TYPE_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw new UnsupportedOperationException(
            "SetOpType not supported: " + setOpProto.getSetOpType());
    }
  }

  private RelFieldCollation translateSortOrder(
      Expression.SortOrder sortOrderProto, int fieldIndex) {

    RelFieldCollation.Direction direction =
        sortOrderProto.getDirection() == Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
            ? RelFieldCollation.Direction.ASCENDING
            : RelFieldCollation.Direction.DESCENDING;
    RelFieldCollation.NullDirection nullDirection;
    switch (sortOrderProto.getNullOrdering()) {
      case SORT_NULLS_FIRST:
        nullDirection = RelFieldCollation.NullDirection.FIRST;
        break;
      case SORT_NULLS_LAST:
        nullDirection = RelFieldCollation.NullDirection.LAST;
        break;
      case UNRECOGNIZED:
        throw new UnsupportedOperationException(
            "Null ordering not recognized: " + sortOrderProto.getNullOrdering());

      case SORT_NULLS_UNSPECIFIED:
      default:
        nullDirection =
            RelFieldCollation.NullDirection.FIRST; // Spark's default - not leaving to Calcite!
    }
    return new RelFieldCollation(fieldIndex, direction, nullDirection);
  }

  private RelNode translateRead(Read readProto) {
    if (readProto.getIsStreaming()) {
      throw new UnsupportedOperationException(
          "Streaming read not supported, in Read relation: " + readProto);
    }

    switch (readProto.getReadTypeCase()) {
      case NAMED_TABLE:
        return translateReadNamedTable(readProto.getNamedTable());
      case DATA_SOURCE:
        return translateReadDataSource(readProto.getDataSource());
      case READTYPE_NOT_SET:
        throw new IllegalArgumentException("Read type not set for Read relation: " + readProto);
      default:
        throw new IllegalArgumentException("Read type not supported in: " + readProto);
    }
  }

  @SuppressWarnings("unused")
  private RelNode translateReadNamedTable(Read.NamedTable readNamedTable) {
    throw new UnsupportedOperationException("Reading named tables not supported");
  }

  /**
   * Converts a Beam Schema into a DDL string (e.g., "col1 INT, col2 VARCHAR").
   *
   * @param schema The Beam Schema to convert.
   * @return A DDL-formatted string representing the schema.
   */
  public static String toDdl(Schema schema) {
    return schema.getFields().stream().map(f -> fieldToDdl(f)).collect(Collectors.joining(", "));
  }

  /** Converts a single Beam Field into a DDL string fragment (e.g., "col1 INT NOT NULL"). */
  private static String fieldToDdl(org.apache.beam.sdk.schemas.Schema.Field field) {
    String typeDdl = fieldTypeToDdl(field.getType());
    String nullability = field.getType().getNullable() ? "" : " NOT NULL";
    return "`" + field.getName() + "` " + typeDdl + nullability;
  }

  /** Recursively converts a Beam FieldType into its DDL string representation. */
  private static String fieldTypeToDdl(FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case STRING:
        return "VARCHAR";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case DOUBLE:
        return "DOUBLE";
      case FLOAT:
        return "FLOAT";
      case INT16:
        return "SMALLINT";
      case BYTE:
        return "TINYINT";
      case BOOLEAN:
        return "BOOLEAN";
      case BYTES:
        return "VARBINARY";
      case DECIMAL:
        return "DECIMAL";
      case DATETIME:
        return "TIMESTAMP";
      case LOGICAL_TYPE:
        //        if ("DATE".equals(fieldType.getLogicalType().getIdentifier())) {
        //          return "DATE";
        //        }
        // Add other logical types here if needed.
        Schema.LogicalType<?, ?> logicalType = checkArgumentNotNull(fieldType.getLogicalType());
        throw new UnsupportedOperationException(
            "Unsupported logical type: " + logicalType.getIdentifier());

        // --- Recursive cases for complex types ---
      case ROW:
        // Recursively convert the nested schema: ROW(field1 TYPE1, field2 TYPE2)
        Schema rowSchema = checkArgumentNotNull(fieldType.getRowSchema());
        return "ROW(" + toDdl(rowSchema) + ")";
      case ARRAY:
        // Recursively convert the element type: ARRAY<element_type>
        FieldType elementType = checkArgumentNotNull(fieldType.getCollectionElementType());
        return "ARRAY<" + fieldTypeToDdl(elementType) + ">";
      case MAP:
        // Recursively convert key and value types: MAP<key_type, value_type>
        FieldType keyType = checkArgumentNotNull(fieldType.getMapKeyType());
        FieldType valueType = checkArgumentNotNull(fieldType.getMapValueType());
        return "MAP<" + fieldTypeToDdl(keyType) + ", " + fieldTypeToDdl(valueType) + ">";
      default:
        throw new UnsupportedOperationException(
            "Unsupported Beam FieldType for DDL conversion: " + fieldType.getTypeName());
    }
  }

  private RelNode translateReadDataSource(Read.DataSource dataSource) {

    String format = dataSource.getFormat().toLowerCase();
    if (format.isEmpty()) {
      // should be filled in with option spark.sql.sources.default
      throw new UnsupportedOperationException("Must set format on data source: " + dataSource);
    }

    // This could be DDL formatted or JSON formatted or absent, in which case we need to infer it;
    // for now make it unsupported
    String schemaString = dataSource.getSchema();
    if (schemaString.isEmpty()) {
      throw new UnsupportedOperationException("Must set schema on data source: " + dataSource);
    }
    Schema beamSchema = parseDataSourceSchema(dataSource.getSchema());

    // No workarounds needed for general JSON support

    String schemaDdl = toDdl(beamSchema);

    // TODO: register providers rather than switch - connect with Beam SQL table provider
    if (!format.equals("csv") && !format.equals("json")) {
      throw new UnsupportedOperationException("Unsupported data source format: " + format);
    }

    String path = dataSource.getPaths(0); // Text table only supports one filepattern - need to
    // chain and use SDF for list of filepatterns
    String tempTableName =
        "temp_" + format + "_read_" + UUID.randomUUID().toString().replace("-", "");

    // this is trash but right now the easiest to way to register a table is to just run DDL
    String tblProperties = String.format("'{\"format\": \"%s\"}'", format);
    String createTableDdl =
        String.format(
            "CREATE EXTERNAL TABLE %s (%s) TYPE 'text' LOCATION '%s' TBLPROPERTIES %s",
            tempTableName, schemaDdl, path, tblProperties);

    beamSqlEnv.executeDdl(createTableDdl);
    RelOptTable relOptTable =
        checkStateNotNull(
            checkStateNotNull(beamSqlEnv.getRelBuilder().getRelOptSchema())
                .getTableForMember(ImmutableList.of(tempTableName)));

    CalciteSchema rootSchema = beamSqlEnv.getContext().getRootSchema();
    List<String> defaultSchemaPath = beamSqlEnv.getContext().getDefaultSchemaPath();
    CalciteSchema defaultSchema =
        checkArgumentNotNull(SqlDdlNodes.childSchema(rootSchema, defaultSchemaPath));

    CatalogManagerSchema catalogManagerSchema = (CatalogManagerSchema) defaultSchema.schema;

    CatalogSchema catalogSchema = catalogManagerSchema.getCurrentCatalogSchema();
    Catalog catalog = catalogSchema.getCatalog();
    Table table = checkStateNotNull(catalog.metaStore("default").getTable(tempTableName));
    BeamSqlTable beamSqlTable =
        checkStateNotNull(catalogSchema.getCatalog().metaStore("default").buildBeamSqlTable(table));

    return new BeamIOSourceRel(
        cluster,
        cluster.traitSetOf(BeamLogicalConvention.INSTANCE),
        relOptTable,
        beamSqlTable,
        beamSqlEnv.getPipelineOptions(),
        BeamCalciteTable.of(beamSqlTable));
  }

  /**
   * Parses a schema string that can be either DDL-formatted or a JSON Avro schema.
   *
   * <p>This method first attempts to parse the string as DDL. If that fails, it falls back to
   * parsing it as a JSON representation of an Avro schema, which is then converted to a Beam
   * Schema. This mimics the behavior of Spark's internal schema parsing.
   *
   * @param schemaString The schema string to parse.
   * @return The parsed Beam {@link Schema}.
   * @throws RuntimeException if the string cannot be parsed as either DDL or JSON.
   */
  public static Schema parseDataSourceSchema(String schemaString) {
    DataType dataType;
    try {
      dataType = DataType.fromJson(schemaString);
    } catch (Exception e) {
      try {
        dataType = DataType.fromDDL(schemaString);
      } catch (Exception e2) {
        throw new RuntimeException("Failed to parse schema as JSON or DDL: " + schemaString, e2);
      }
    }

    LOG.info("Here's the type: {}", dataType);

    return sparkStructTypeToBeamSchema((StructType) dataType);
    //    try {
    //      // First, try to parse the string as a Beam SQL DDL statement.
    //      SqlNode node = beamSqlEnv.getPlanner().parse(schemaString);
    //      if (node.getKind().belongsTo(SqlKind.DDL)) {
    //        throw new UnsupportedOperationException(
    //            "Have DDL but don't knwo what to do with it yet: " + schemaString);
    //      }
    //    } catch (ParseException ddlException) {
    //      try {
    //        // If DDL parsing fails, try to parse it as a JSON Avro schema.
    //        org.apache.avro.Schema avroSchema = new
    // org.apache.avro.Schema.Parser().parse(schemaString);
    //        // Convert the Avro schema to a Beam schema.
    //        return AvroUtils.toBeamSchema(avroSchema);
    //      } catch (Exception jsonException) {
    //        // If both DDL and JSON parsing fail, throw an exception that includes both causes.
    //        RuntimeException combinedException =
    //            new RuntimeException(
    //                "Failed to parse schema string as either DDL or JSON: " + schemaString);
    //        combinedException.addSuppressed(ddlException);
    //        combinedException.addSuppressed(jsonException);
    //        throw combinedException;
    //      }
    //    }
    //    throw new IllegalArgumentException("Could not parse schema as DDL or JSON: " +
    // schemaString);
  }

  /**
   * Converts a Spark {@link StructType} to a Beam {@link Schema}.
   *
   * @param sparkSchema The input Spark schema.
   * @return The corresponding Beam Schema.
   */
  private static Schema sparkStructTypeToBeamSchema(StructType sparkSchema) {
    Schema.Builder beamSchemaBuilder = Schema.builder();
    for (StructField sparkField : sparkSchema.fields()) {
      FieldType beamFieldType = fromSparkType(sparkField.dataType());

      // Add the field to the builder, preserving its nullability.
      if (sparkField.nullable()) {
        beamSchemaBuilder.addNullableField(sparkField.name(), beamFieldType);
      } else {
        beamSchemaBuilder.addField(sparkField.name(), beamFieldType);
      }
    }
    return beamSchemaBuilder.build();
  }

  /** Recursively converts a Spark {@link DataType} to a Beam {@link FieldType}. */
  private static FieldType fromSparkType(DataType sparkType) {
    // For simple types, we can use the static instances from Spark's DataTypes class.
    if (sparkType.equals(DataTypes.StringType)) {
      return FieldType.STRING;
    } else if (sparkType.equals(DataTypes.IntegerType)) {
      return FieldType.INT32;
    } else if (sparkType.equals(DataTypes.LongType)) {
      return FieldType.INT64;
    } else if (sparkType.equals(DataTypes.DoubleType)) {
      return FieldType.DOUBLE;
    } else if (sparkType.equals(DataTypes.FloatType)) {
      return FieldType.FLOAT;
    } else if (sparkType.equals(DataTypes.ShortType)) {
      return FieldType.INT16;
    } else if (sparkType.equals(DataTypes.ByteType)) {
      return FieldType.BYTE;
    } else if (sparkType.equals(DataTypes.BooleanType)) {
      return FieldType.BOOLEAN;
    } else if (sparkType.equals(DataTypes.BinaryType)) {
      return FieldType.BYTES;
    } else if (sparkType.equals(DataTypes.DateType)) {
      // Beam uses LogicalTypes for more specific date/time representations.
      // We could use SqlTypes.Date() but DATETIME covers it for now.
      return FieldType.DATETIME;
    } else if (sparkType.equals(DataTypes.TimestampType)) {
      return FieldType.DATETIME;
    } else if (sparkType.equals(DataTypes.TimestampNTZType)) {
      return FieldType.DATETIME;
    } else if (sparkType.equals(DataTypes.NullType)) {
      return FieldType.STRING; // Fallback for nulls
    } else if (sparkType instanceof DecimalType) {
      // Beam's DECIMAL type is generic. A more advanced conversion could
      // potentially use this precision/scale for validation.
      // DecimalType decimalType = (DecimalType) sparkType;
      // decimalType.precision();
      // decimalType.scale();
      return FieldType.DECIMAL;
    } else if (sparkType instanceof ArrayType) {
      // For complex types, we need to handle them recursively.
      ArrayType arrayType = (ArrayType) sparkType;
      FieldType elementType = fromSparkType(arrayType.elementType());
      // The `containsNull` property maps to the nullability of the collection's element type.
      return FieldType.array(elementType.withNullable(arrayType.containsNull()));
    } else if (sparkType instanceof MapType) {
      MapType mapType = (MapType) sparkType;
      FieldType keyType = fromSparkType(mapType.keyType());
      FieldType valueType = fromSparkType(mapType.valueType());
      // Maps in Beam schemas can have nullable values.
      return FieldType.map(keyType, valueType.withNullable(mapType.valueContainsNull()));
    } else if (sparkType instanceof StructType) {
      // Recursively convert nested structs.
      return FieldType.row(sparkStructTypeToBeamSchema((StructType) sparkType));
    } else {
      throw new UnsupportedOperationException(
          "Unsupported Spark DataType: " + sparkType.typeName());
    }
  }

  private RelNode translateSort(Sort sortProto) {
    RelNode input = translate(sortProto.getInput());
    SparkExpressionToRexNode expressionToRexNode =
        new SparkExpressionToRexNode(cluster, input.getRowType(), beamSqlEnv.getOperatorTable());

    List<RelFieldCollation> collations = new ArrayList<>();

    for (Expression.SortOrder order : sortProto.getOrderList()) {
      // For now, we only support sorting by a direct column reference.
      // Support for sorting by arbitrary expressions would require a project-sort-project pattern.
      Expression sortExpression = order.getChild();
      if (sortExpression.getExprTypeCase() != Expression.ExprTypeCase.UNRESOLVED_ATTRIBUTE) {
        throw new UnsupportedOperationException(
            "Sorting by complex expressions is not yet supported. Found: "
                + sortExpression.getExprTypeCase());
      }

      RexInputRef fieldRef = (RexInputRef) expressionToRexNode.translate(sortExpression);
      int fieldIndex = fieldRef.getIndex();

      // Create the collation for this field.
      collations.add(translateSortOrder(order, fieldIndex));
    }

    // Create the LogicalSort directly on the input.
    return LogicalSort.create(input, RelCollations.of(collations), null, null);
  }

  private RelNode translateLimit(Limit limitProto) {
    RelNode input = translate(limitProto.getInput());
    RexNode limit =
        cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(limitProto.getLimit()));
    return LogicalSort.create(input, RelCollations.EMPTY, null, limit);
  }

  private RelNode translateOffset(Offset offsetProto) {
    RelNode input = translate(offsetProto.getInput());
    RexNode offset =
        cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(offsetProto.getOffset()));
    return LogicalSort.create(input, RelCollations.EMPTY, offset, null);
  }

  @SuppressWarnings("unused")
  private RelNode translateTail(Tail tailProto) {
    // TAIL N is equivalent to ORDER BY all columns DESC (for some stable order) and then LIMIT N,
    // but applied from the end. This is not standard SQL and hard to map directly.
    // Or, if total count is known, it's OFFSET (COUNT - N).
    return unsupported("Tail");
  }

  /**
   * Translates a Spark Aggregate into a Calcite LogicalAggregate, inserting a projection for
   * casting if necessary to match Spark SQL semantics (for example AVG in Spark SQL always widens
   * the type, whereas in Calcite and many SQL databases an INT input would cause an INT output for
   * the AVG).
   */
  private RelNode translateAggregate(Aggregate aggProto) {
    RelNode originalInput = translate(aggProto.getInput());
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

    List<RexNode> projectionExprs = new ArrayList<>();
    originalInput
        .getRowType()
        .getFieldList()
        .forEach(
            field ->
                projectionExprs.add(rexBuilder.makeInputRef(field.getType(), field.getIndex())));

    boolean needsProjection = false;
    Map<Integer, RexNode> castArgs = new HashMap<>();

    // 1. Process grouping expressions
    List<Integer> groupKeyIndices = new ArrayList<>();
    SparkExpressionToRexNode origExprConverter =
        new SparkExpressionToRexNode(
            cluster, originalInput.getRowType(), beamSqlEnv.getOperatorTable());

    for (Expression groupExpr : aggProto.getGroupingExpressionsList()) {
      RexNode groupNode = origExprConverter.translate(groupExpr);
      if (groupNode instanceof RexInputRef) {
        groupKeyIndices.add(((RexInputRef) groupNode).getIndex());
      } else {
        int index = projectionExprs.size();
        projectionExprs.add(groupNode);
        needsProjection = true;
        groupKeyIndices.add(index);
      }
    }

    // 2. Process aggregate calls
    List<List<Integer>> aggCallArgs = new ArrayList<>();
    for (Expression aggExpr : aggProto.getAggregateExpressionsList()) {
      Expression funcExpr = aggExpr;
      if (aggExpr.getExprTypeCase() == Expression.ExprTypeCase.ALIAS) {
        funcExpr = aggExpr.getAlias().getExpr();
      }

      if (funcExpr.getExprTypeCase() != Expression.ExprTypeCase.UNRESOLVED_FUNCTION) {
        throw new UnsupportedOperationException(
            "Unsupported agg expression type: " + funcExpr.getExprTypeCase());
      }
      Expression.UnresolvedFunction func = funcExpr.getUnresolvedFunction();
      SqlAggFunction aggFunction = AGG_OPERATOR_MAP.get(func.getFunctionName().toLowerCase());

      if (aggFunction == null) {
        throw new UnsupportedOperationException(
            "Unsupported agg function: " + func.getFunctionName());
      }

      List<Integer> argList = new ArrayList<>();
      for (Expression arg : func.getArgumentsList()) {
        RexNode argNode = origExprConverter.translate(arg);
        int argIndex;
        if (argNode instanceof RexInputRef) {
          argIndex = ((RexInputRef) argNode).getIndex();
        } else {
          argIndex = projectionExprs.size();
          projectionExprs.add(argNode);
          needsProjection = true;
        }

        RelDataType actualType = argNode.getType();
        RelDataType expectedType = getExpectedOperandType(aggFunction, typeFactory);

        if (expectedType != null
            && !actualType.equals(expectedType)
            && !castArgs.containsKey(argIndex)) {

          needsProjection = true;
          RexNode projectedNode = projectionExprs.get(argIndex);
          RexNode castNode;
          if (projectedNode instanceof RexInputRef) {
            castNode =
                rexBuilder.makeCast(expectedType, rexBuilder.makeInputRef(actualType, argIndex));
          } else {
            castNode = rexBuilder.makeCast(expectedType, projectedNode);
          }
          projectionExprs.set(argIndex, castNode);
          castArgs.put(argIndex, castNode);
        }
        argList.add(argIndex);
      }
      aggCallArgs.add(argList);
    }

    RelNode aggInput = originalInput;
    if (needsProjection) {
      List<String> fieldNames = new ArrayList<>(originalInput.getRowType().getFieldNames());
      for (int i = fieldNames.size(); i < projectionExprs.size(); i++) {
        fieldNames.add("EXPR$" + i);
      }
      aggInput =
          LogicalProject.create(originalInput, ImmutableList.of(), projectionExprs, fieldNames);
    }

    ImmutableBitSet groupSet = ImmutableBitSet.of(groupKeyIndices);

    List<AggregateCall> aggCalls = new ArrayList<>();
    int i = 0;
    for (Expression aggExpr : aggProto.getAggregateExpressionsList()) {
      Expression funcExpr = aggExpr;
      String aliasName = null;
      if (aggExpr.getExprTypeCase() == Expression.ExprTypeCase.ALIAS) {
        funcExpr = aggExpr.getAlias().getExpr();
        aliasName = aggExpr.getAlias().getName(0);
      }
      Expression.UnresolvedFunction func = funcExpr.getUnresolvedFunction();
      SqlAggFunction aggFunction = AGG_OPERATOR_MAP.get(func.getFunctionName().toLowerCase());

      if (aggFunction == null) {
        throw new UnsupportedOperationException(
            "unsupported agg function " + func.getFunctionName());
      }

      List<Integer> argList = aggCallArgs.get(i++);
      RelNode finalAggInput = aggInput;
      RelDataType aggFuncType =
          deriveAggType(
              aggFunction,
              argList.stream()
                  .map(idx -> finalAggInput.getRowType().getFieldList().get(idx).getType())
                  .collect(Collectors.toList()));

      aggCalls.add(
          AggregateCall.create(
              aggFunction,
              func.getIsDistinct(),
              false,
              false,
              argList,
              -1,
              RelCollations.EMPTY,
              aggFuncType,
              aliasName));
    }

    return LogicalAggregate.create(aggInput, groupSet, ImmutableList.of(groupSet), aggCalls);
  }

  /**
   * A generic method to specify the expected operand type for certain aggregate functions. This
   * bridges the gap between the logical plan and the physical execution requirements.
   *
   * @return The expected RelDataType, or null if no special requirement is known.
   */
  private @Nullable RelDataType getExpectedOperandType(
      SqlAggFunction aggFunction, RelDataTypeFactory typeFactory) {
    // For AVG, the Beam runner's implementation expects a DOUBLE.
    if (aggFunction.getKind() == SqlKind.AVG) {
      return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    }

    // For SUM on integers, a BIGINT might be required to prevent overflow,
    // but for now we won't force a cast. This can be added here if needed.
    // if (aggFunction.getKind() == SqlKind.SUM) { ... }

    // By default, no cast is required.
    return null;
  }

  /**
   * A helper method to derive the return type of an aggregate function. This is a simplified
   * version; a full implementation would use Calcite's type inference.
   */
  private RelDataType deriveAggType(SqlAggFunction aggFunction, List<RelDataType> argTypes) {
    // TODO: calcite should have a call for us, somewhere around
    // https://github.com/apache/calcite/blob/92a1028d65efc3005eb22c3def97adefd9e8f2fc/core/src/main/java/org/apache/calcite/rel/type/RelDataTypeSystemImpl.java#L368
    if (aggFunction.getKind() == SqlKind.COUNT) {
      return cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    } else if (aggFunction.getKind() == SqlKind.AVG) {
      // AVG can return a wider type than its input, e.g., DECIMAL or DOUBLE.
      // For simplicity, we'll return DOUBLE here.

      // In Calcite SQL the return type of AVG is the same as its argument type. users have to cast
      // to DOUBLE if they want a DOUBLE precision return
      return argTypes.get(0);
      // return cluster.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
    } else if (!argTypes.isEmpty()) {
      // For many aggregates (SUM, MIN, MAX), the return type is the same as the input type.
      return argTypes.get(0);
    }
    // Fallback for functions like COUNT(*) which have no arguments.
    return cluster.getTypeFactory().createSqlType(SqlTypeName.ANY);
  }

  private RelNode translateDeduplicate(Deduplicate dedupeProto) {
    RelNode input = translate(dedupeProto.getInput());
    boolean dedupeAll =
        dedupeProto.getAllColumnsAsKeys() || dedupeProto.getColumnNamesList().isEmpty();
    if (!dedupeAll) {
      throw new UnsupportedOperationException(
          "Deduplicate with specific columns not supported yet");
    }
    return org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalAggregate
        .create(
            input,
            Collections.emptyList(),
            org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.ImmutableBitSet.range(
                input.getRowType().getFieldCount()),
            null,
            Collections.emptyList());
  }

  private RelNode translateRange(Range rangeProto) {
    long start = rangeProto.getStart();
    long end = rangeProto.getEnd();
    long step = rangeProto.getStep();
    if (step == 0) throw new IllegalArgumentException("Range step cannot be zero.");

    RelDataType rowType =
        cluster
            .getTypeFactory()
            .createStructType(
                ImmutableList.of(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
                ImmutableList.of("id"));

    ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
    if (step > 0) {
      for (long i = start; i < end; i += step) {
        tuples.add(
            ImmutableList.of(cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(i))));
      }
    } else { // step < 0
      for (long i = start; i > end; i += step) {
        tuples.add(
            ImmutableList.of(cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(i))));
      }
    }
    return LogicalValues.create(cluster, rowType, tuples.build());
  }

  private RelNode translateSql(SQL sqlProto) {
    String sql = sqlProto.getQuery();
    // Preprocess SQL to handle Spark-specific syntax that Calcite doesn't like
    // Handle: SELECT * FROM VALUES (...) AS tab(...) -> SELECT * FROM (VALUES (...)) AS tab(...)
    if (sql.toUpperCase().contains("FROM VALUES") && sql.toUpperCase().contains(" AS ")) {
      sql = sql.replaceAll("(?i)FROM\\s+VALUES\\b([\\s\\S]*?)\\bAS\\b", "FROM (VALUES $1) AS");
    }
    // Standardize Spark literal constructors to Calcite syntax
    sql = sql.replaceAll("(?i)\\bDATE\\s*\\(\\s*'([^']*)'\\s*\\)", "DATE '$1'");
    sql = sql.replaceAll("(?i)\\bTIMESTAMP\\s*\\(\\s*'([^']*)'\\s*\\)", "TIMESTAMP '$1'");

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

    return beamSqlEnv.parseQuery(sql);
  }

  @SuppressWarnings("unused")
  private RelNode translateDrop(Drop dropProto) {
    RelNode input = translate(dropProto.getInput());
    List<String> dropNames = dropProto.getColumnNamesList();
    // TODO: Handle dropProto.getColumns() expressions

    List<RexNode> projections = new ArrayList<>();
    List<String> newFieldNames = new ArrayList<>();
    for (RelDataTypeField field : input.getRowType().getFieldList()) {
      if (!dropNames.contains(field.getName())) {
        projections.add(cluster.getRexBuilder().makeInputRef(field.getType(), field.getIndex()));
        newFieldNames.add(field.getName());
      }
    }
    return LogicalProject.create(input, Collections.emptyList(), projections, newFieldNames);
  }

  private RelNode translateWithColumnsRenamed(WithColumnsRenamed renameProto) {
    RelNode input = translate(renameProto.getInput());
    RexBuilder rexBuilder = cluster.getRexBuilder();
    List<RexNode> projects = new ArrayList<>();
    List<String> newNames = new ArrayList<>(input.getRowType().getFieldNames());

    for (int i = 0; i < input.getRowType().getFieldCount(); i++) {
      projects.add(rexBuilder.makeInputRef(input, i));
    }

    for (WithColumnsRenamed.Rename rename : renameProto.getRenamesList()) {
      for (int i = 0; i < newNames.size(); i++) {
        if (newNames.get(i).equals(rename.getColName())) {
          newNames.set(i, rename.getNewColName());
        }
      }
    }

    for (Map.Entry<String, String> entry : renameProto.getRenameColumnsMapMap().entrySet()) {
      for (int i = 0; i < newNames.size(); i++) {
        if (newNames.get(i).equals(entry.getKey())) {
          newNames.set(i, entry.getValue());
        }
      }
    }

    return LogicalProject.create(
        input, Collections.emptyList(), projects, newNames, Collections.emptySet());
  }

  private RelNode translateWithColumns(WithColumns withColumnsProto) {
    RelNode input = translate(withColumnsProto.getInput());
    RexBuilder rexBuilder = cluster.getRexBuilder();
    List<RexNode> projects = new ArrayList<>();
    List<String> newNames = new ArrayList<>(input.getRowType().getFieldNames());

    for (int i = 0; i < input.getRowType().getFieldCount(); i++) {
      projects.add(rexBuilder.makeInputRef(input, i));
    }

    for (org.apache.spark.connect.proto.Expression.Alias alias :
        withColumnsProto.getAliasesList()) {
      String colName = alias.getName(0);
      RexNode expr =
          new SparkExpressionToRexNode(cluster, input.getRowType(), beamSqlEnv.getOperatorTable())
              .translate(alias.getExpr());
      int idx = newNames.indexOf(colName);
      if (idx >= 0) {
        projects.set(idx, expr);
      } else {
        projects.add(expr);
        newNames.add(colName);
      }
    }

    return LogicalProject.create(
        input, Collections.emptyList(), projects, newNames, Collections.emptySet());
  }

  private RelNode translateToDf(ToDF toDfProto) {
    RelNode input = translate(toDfProto.getInput());
    List<String> newNames = toDfProto.getColumnNamesList();
    if (newNames.size() != input.getRowType().getFieldCount()) {
      throw new IllegalArgumentException(
          "ToDF column names count must match input column count. "
              + "Input: "
              + input.getRowType().getFieldCount()
              + " ToDF: "
              + newNames.size());
    }

    RexBuilder rexBuilder = cluster.getRexBuilder();
    List<RexNode> projects = new ArrayList<>();
    for (int i = 0; i < input.getRowType().getFieldCount(); i++) {
      projects.add(rexBuilder.makeInputRef(input, i));
    }

    return LogicalProject.create(
        input, Collections.emptyList(), projects, newNames, Collections.emptySet());
  }

  private RelDataType arrowFieldToSqlType(Field field, JavaTypeFactory typeFactory) {
    ArrowType arrowType = field.getType();
    LOG.info("Arrow type for field {}: {}", field.getName(), arrowType);
    RelDataType type;
    if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
      type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    } else if (arrowType instanceof ArrowType.Null) {
      type = typeFactory.createSqlType(SqlTypeName.NULL);
    } else if (arrowType instanceof ArrowType.Binary
        || arrowType instanceof ArrowType.FixedSizeBinary) {
      type = typeFactory.createSqlType(SqlTypeName.VARBINARY);
    } else if (arrowType instanceof ArrowType.Map) {
      Field structField = field.getChildren().get(0);
      Field keyField = structField.getChildren().get(0);
      Field valueField = structField.getChildren().get(1);
      RelDataType keyType = arrowFieldToSqlType(keyField, typeFactory);
      RelDataType valueType = arrowFieldToSqlType(valueField, typeFactory);
      RelDataType structType =
          typeFactory.createStructType(
              ImmutableList.of(keyType, valueType), ImmutableList.of("key", "value"));
      type = typeFactory.createArrayType(structType, -1);
    } else if (arrowType instanceof ArrowType.List) {
      Field elementField = field.getChildren().get(0);
      RelDataType elementType = arrowFieldToSqlType(elementField, typeFactory);
      type = typeFactory.createArrayType(elementType, -1);
    } else if (arrowType instanceof ArrowType.Struct) {
      final RelDataTypeFactory.Builder structBuilder = typeFactory.builder();
      for (Field childField : field.getChildren()) {
        structBuilder.add(childField.getName(), arrowFieldToSqlType(childField, typeFactory));
      }
      type = structBuilder.build();
    } else if (arrowType instanceof ArrowType.Interval) {
      ArrowType.Interval intervalType = (ArrowType.Interval) arrowType;
      if (intervalType.getUnit() == org.apache.arrow.vector.types.IntervalUnit.DAY_TIME) {
        LOG.info("Mapping Arrow Interval(DAY_TIME) to SqlTypeName.INTERVAL_DAY_SECOND");
        type = typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_SECOND);
      } else {
        LOG.info("Mapping Arrow Interval(non-DAY_TIME) to SqlTypeName.VARCHAR");
        type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      }
    } else if (arrowType instanceof ArrowType.Duration) {
      LOG.info("Mapping Arrow Duration to SqlTypeName.INTERVAL_DAY_SECOND");
      type = typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_SECOND);
    } else {
      try {
        type = ArrowFieldTypeFactory.toType(arrowType, typeFactory);
        if (type.getSqlTypeName() == SqlTypeName.REAL) {
          type = typeFactory.createSqlType(SqlTypeName.FLOAT);
        }
      } catch (Exception e) {
        LOG.warn("Unsupported Arrow type: {}, falling back to VARCHAR", arrowType, e);
        type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      }
    }
    boolean nullable = field.isNullable();
    if ("__none__".equals(field.getName())
        && type.getSqlTypeName()
            == org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName
                .BOOLEAN) {
      nullable = false;
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  private RelDataType arrowSchemaToRowType(
      org.apache.arrow.vector.types.pojo.Schema schema, JavaTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    java.util.Set<String> seenNames = new java.util.HashSet<>();
    for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
      String name = field.getName();
      ArrowType arrowType = field.getType();
      if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
        org.apache.arrow.vector.types.pojo.ArrowType.Timestamp tsType =
            (org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) arrowType;

        if (tsType.getTimezone() == null || tsType.getTimezone().isEmpty()) {
          name = name + "__ntz";
        }
      }
      int counter = 0;
      String baseName = name;
      while (seenNames.contains(name)) {
        name = baseName + "_" + counter++;
      }
      seenNames.add(name);
      builder.add(name, arrowFieldToSqlType(field, typeFactory));
    }
    return builder.build();
  }

  /**
   * Pivots column-oriented Arrow VectorSchemaRoot into Calcite-friendly literal rows. Not suitable
   * for large scale, but useful for small literal dataframes/relations.
   */
  private void addRows(
      ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder,
      RexBuilder rexBuilder,
      VectorSchemaRoot root,
      RelDataType rowType) {

    int rowCount = root.getRowCount();
    if (rowCount == 0) {
      return;
    }

    List<Field> arrowFields = root.getSchema().getFields();
    List<FieldVector> vectors = root.getFieldVectors();

    for (int i = 0; i < rowCount; i++) {
      ImmutableList.Builder<RexLiteral> rowBuilder = ImmutableList.builder();

      for (int j = 0; j < vectors.size(); j++) {
        FieldVector vector = vectors.get(j);
        Object javaValue = vector.getObject(i);

        RelDataType fieldType = rowType.getFieldList().get(j).getType();
        ArrowType arrowType = arrowFields.get(j).getType();

        RexLiteral literal = createRexLiteral(rexBuilder, javaValue, fieldType, arrowType);
        rowBuilder.add(literal);
      }
      tuplesBuilder.add(rowBuilder.build());
    }
  }

  private RexLiteral createRexLiteral(
      RexBuilder rexBuilder,
      Object javaValue,
      RelDataType relDataType,
      @Nullable ArrowType arrowType) {
    if (javaValue == null) {
      checkArgument(
          relDataType.isNullable(), "Received null arrow value for non-nullable Calcite type");
      return rexBuilder.makeNullLiteral(relDataType);
    }

    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();

    // TODO: check these details more closely
    switch (sqlTypeName) {
      case VARCHAR:
      case CHAR:
        // TODO: maybe there's a better way to toString an arrow Text object
        return rexBuilder.makeLiteral(javaValue.toString(), relDataType);
      case BOOLEAN:
        return rexBuilder.makeLiteral(javaValue, relDataType);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(((Number) javaValue).longValue()), relDataType);
      case FLOAT: // Calcite FLOAT is 4 bytes
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) javaValue).floatValue()));
      case REAL: // Typically synonym for FLOAT
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) javaValue).floatValue()));
      case DOUBLE:
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) javaValue).doubleValue()));
      case DECIMAL:
        return rexBuilder.makeExactLiteral((BigDecimal) javaValue, relDataType);
      case DATE:
        // Arrow DateDayVector -> Integer (days since epoch)
        if (javaValue instanceof Integer) {
          return rexBuilder.makeDateLiteral(
              org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.DateString
                  .fromDaysSinceEpoch((Integer) javaValue));
        } else if (javaValue instanceof Number) {
          return rexBuilder.makeDateLiteral(
              org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.DateString
                  .fromDaysSinceEpoch(((Number) javaValue).intValue()));
        }
        break;
      case TIME:
        // Arrow Time(Nano/Micro/Milli/Sec)Vector -> Long
        // Calcite TIME literal (precision is for fractional seconds)
        // Example: TIME without timezone
        // Needs conversion from nanos/micros/etc. of day to MillisTimeString
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIME_TZ:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_TZ:
      case TIMESTAMP:
        if (javaValue instanceof Long) {
          if (arrowType == null) {
            throw new IllegalStateException("ArrowType is null for Timestamp");
          }
          long epochMillis = translateArrowTimestampToMillis((Long) javaValue, arrowType);
          int precision =
              relDataType.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                  ? 3
                  : relDataType.getPrecision();
          return rexBuilder.makeTimestampLiteral(
              org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.TimestampString
                  .fromMillisSinceEpoch(epochMillis),
              precision);
        } else if (javaValue instanceof java.time.LocalDateTime) {
          java.time.LocalDateTime ldt = (java.time.LocalDateTime) javaValue;
          String ldtStr = ldt.toString().replace('T', ' ');
          int precision =
              relDataType.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                  ? 6
                  : relDataType.getPrecision();
          return rexBuilder.makeTimestampLiteral(
              new org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.TimestampString(
                  ldtStr),
              precision);
        }
        break;
      case BINARY:
      case VARBINARY:
        if (javaValue instanceof byte[]) {
          return rexBuilder.makeBinaryLiteral(
              new org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString(
                  (byte[]) javaValue));
        } else if (javaValue instanceof org.apache.arrow.vector.util.Text) {
          return rexBuilder.makeBinaryLiteral(
              new org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString(
                  ((org.apache.arrow.vector.util.Text) javaValue).getBytes()));
        }
        return rexBuilder.makeNullLiteral(relDataType);
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
      case NULL:
      case UNKNOWN:
      case SYMBOL:
      case MULTISET:
      case ARRAY:
      case MAP:
      case DISTINCT:
      case STRUCTURED:
      case ROW:
      case ANY:
        return rexBuilder.makeNullLiteral(relDataType);
      case OTHER:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case MEASURE:
      case FUNCTION:
      case SARG:
      case UUID:
      case VARIANT:
      default:
        break;
    }
    throw new UnsupportedOperationException(
        "RexLiteral conversion not implemented for: "
            + sqlTypeName
            + " from Arrow type "
            + arrowType);
  }

  private long translateArrowTimestampToMillis(long rawValue, ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
      switch (tsType.getUnit()) {
        case MILLISECOND:
          return rawValue;
        case MICROSECOND:
          return rawValue / 1000L;
        case NANOSECOND:
          return rawValue / 1000000L;
        case SECOND:
          return rawValue * 1000L;
      }
    }
    throw new IllegalArgumentException("Unsupported Timestamp unit in Arrow type: " + arrowType);
  }

  private RexNode createRexNode(
      RexBuilder rexBuilder,
      @Nullable Object javaValue,
      RelDataType relDataType,
      @Nullable ArrowType arrowType) {
    if (javaValue == null) {
      return rexBuilder.makeNullLiteral(relDataType);
    }

    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    if (sqlTypeName == SqlTypeName.ARRAY) {
      List<RexNode> elements = new ArrayList<>();
      RelDataType componentType = relDataType.getComponentType();
      if (componentType == null) {
        throw new IllegalStateException("Component type of ARRAY is null");
      }
      if (javaValue instanceof List) {
        for (Object element : (List<?>) javaValue) {
          elements.add(createRexNode(rexBuilder, element, componentType, null));
        }
      }
      return rexBuilder.makeCall(
          relDataType,
          org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable
              .ARRAY_VALUE_CONSTRUCTOR,
          elements);
    } else if (relDataType.isStruct()) {
      List<RexNode> fieldNodes = new ArrayList<>();
      List<RelDataTypeField> fields = relDataType.getFieldList();
      if (javaValue instanceof Map) {
        Map<?, ?> mapValue = (Map<?, ?>) javaValue;
        for (RelDataTypeField field : fields) {
          Object val = mapValue.get(field.getName());
          fieldNodes.add(createRexNode(rexBuilder, val, field.getType(), null));
        }
      }
      return rexBuilder.makeCall(
          relDataType,
          org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW,
          fieldNodes);
    } else if (sqlTypeName == SqlTypeName.MAP) {
      List<RexNode> operands = new ArrayList<>();
      RelDataType keyType = relDataType.getKeyType();
      RelDataType valueType = relDataType.getValueType();
      if (keyType == null || valueType == null) {
        throw new IllegalStateException("Key or Value type of MAP is null");
      }
      if (javaValue instanceof List) {
        for (Object entry : (List<?>) javaValue) {
          if (entry instanceof Map) {
            Map<?, ?> mapEntry = (Map<?, ?>) entry;
            operands.add(createRexNode(rexBuilder, mapEntry.get("key"), keyType, null));
            operands.add(createRexNode(rexBuilder, mapEntry.get("value"), valueType, null));
          }
        }
      }
      return rexBuilder.makeCall(
          relDataType,
          org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable
              .MAP_VALUE_CONSTRUCTOR,
          operands);
    }

    return createRexLiteral(rexBuilder, javaValue, relDataType, arrowType);
  }

  private void addRowsAsProjects(
      List<RelNode> projects,
      RexBuilder rexBuilder,
      VectorSchemaRoot root,
      RelDataType rowType,
      RelNode dummyValues) {

    int rowCount = root.getRowCount();
    if (rowCount == 0) {
      return;
    }

    List<org.apache.arrow.vector.types.pojo.Field> arrowFields = root.getSchema().getFields();
    List<FieldVector> vectors = root.getFieldVectors();

    for (int i = 0; i < rowCount; i++) {
      List<RexNode> rowProjects = new ArrayList<>();

      for (int j = 0; j < vectors.size(); j++) {
        FieldVector vector = vectors.get(j);
        Object javaValue = vector.getObject(i);

        RelDataType fieldType = rowType.getFieldList().get(j).getType();
        ArrowType arrowType = arrowFields.get(j).getType();

        RexNode node = createRexNode(rexBuilder, javaValue, fieldType, arrowType);
        rowProjects.add(node);
      }

      RelNode project =
          LogicalProject.create(
              dummyValues,
              Collections.emptyList(),
              rowProjects,
              rowType.getFieldNames(),
              Collections.emptySet());
      projects.add(project);
    }
  }

  private boolean hasComplexTypes(RelDataType rowType) {
    for (org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeField field :
        rowType.getFieldList()) {
      org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName typeName =
          field.getType().getSqlTypeName();
      if (typeName
              == org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName
                  .ARRAY
          || typeName
              == org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName.MAP
          || field.getType().isStruct()) {
        return true;
      }
    }
    return false;
  }

  private RelNode translateLocalRelation(LocalRelation localRelation) {
    if (!localRelation.hasData()) {
      throw new UnsupportedOperationException(
          "LocalRelation must have `data` field. "
              + "Parsing Spark SQL DDL or JSON type representation is not supported.");
    }

    String limitStr = conf.get("spark.sql.session.localRelationSizeLimit");
    long limit = limitStr != null ? Long.parseLong(limitStr) : 64 * 1024 * 1024;
    long size = localRelation.getData().size();
    if (size > limit) {
      throw new RuntimeException(
          "[LOCAL_RELATION_SIZE_LIMIT_EXCEEDED] Local relation size exceeds limit: "
              + size
              + " > "
              + limit);
    }

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ByteArrayInputStream arrowBytesInputStream =
          new ByteArrayInputStream(localRelation.getData().toByteArray());

      try (ArrowStreamReader streamReader =
          new ArrowStreamReader(arrowBytesInputStream, allocator)) {
        VectorSchemaRoot root = streamReader.getVectorSchemaRoot();
        RelDataType rowType =
            arrowSchemaToRowType(root.getSchema(), (JavaTypeFactory) cluster.getTypeFactory());

        if (hasComplexTypes(rowType)) {
          List<RelNode> projects = new ArrayList<>();
          RelDataType emptyRowType =
              cluster
                  .getTypeFactory()
                  .createStructType(Collections.emptyList(), Collections.emptyList());
          org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalValues
              dummyValues =
                  org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical
                      .LogicalValues.create(
                      cluster, emptyRowType, ImmutableList.of(ImmutableList.of()));

          addRowsAsProjects(projects, cluster.getRexBuilder(), root, rowType, dummyValues);
          while (streamReader.loadNextBatch()) {
            addRowsAsProjects(projects, cluster.getRexBuilder(), root, rowType, dummyValues);
          }

          if (projects.isEmpty()) {
            return org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical
                .LogicalValues.create(cluster, rowType, ImmutableList.of());
          } else if (projects.size() == 1) {
            return projects.get(0);
          } else {
            return LogicalUnion.create(projects, true);
          }
        } else {
          ImmutableList.Builder<
                  ImmutableList<
                      org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral>>
              tuplesBuilder = ImmutableList.builder();
          addRows(tuplesBuilder, cluster.getRexBuilder(), root, rowType);
          while (streamReader.loadNextBatch()) {
            addRows(tuplesBuilder, cluster.getRexBuilder(), root, rowType);
          }
          return org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalValues
              .create(cluster, rowType, tuplesBuilder.build());
        }
      } catch (IOException exc) {
        throw new RuntimeException("Failed to parse arrow data for LocalRelation", exc);
      }
    }
  }

  private List<Row> arrowToBeamRows(VectorSchemaRoot root, Schema schema) {
    List<Row> rows = new ArrayList<>();
    int rowCount = root.getRowCount();
    List<FieldVector> vectors = root.getFieldVectors();
    for (int i = 0; i < rowCount; i++) {
      List<@Nullable Object> values = new ArrayList<>();
      for (int j = 0; j < vectors.size(); j++) {
        FieldVector vector = vectors.get(j);
        Object val = vector.getObject(i);
        values.add(convertArrowValueToBeam(val, schema.getField(j).getType()));
      }
      rows.add(Row.withSchema(schema).addValues(values).build());
    }
    return rows;
  }

  private @Nullable Object convertArrowValueToBeam(@Nullable Object val, FieldType type) {
    if (val == null) {
      return null;
    }
    if (type.getTypeName() == Schema.TypeName.ROW) {
      Map<String, Object> map = (Map<String, Object>) val;
      Schema structSchema = checkArgumentNotNull(type.getRowSchema());
      List<@Nullable Object> values = new ArrayList<>();
      for (int i = 0; i < structSchema.getFieldCount(); i++) {
        Schema.Field field = structSchema.getField(i);
        values.add(convertArrowValueToBeam(map.get(field.getName()), field.getType()));
      }
      return Row.withSchema(structSchema).addValues(values).build();
    }
    if (type.getTypeName() == Schema.TypeName.ARRAY) {
      Iterable<?> iterable = (Iterable<?>) val;
      FieldType elementType = checkArgumentNotNull(type.getCollectionElementType());
      List<@Nullable Object> convertedList = new ArrayList<>();
      for (Object elem : iterable) {
        convertedList.add(convertArrowValueToBeam(elem, elementType));
      }
      return convertedList;
    }
    if (type.getTypeName() == Schema.TypeName.LOGICAL_TYPE) {
      Schema.LogicalType<?, ?> logicalType = type.getLogicalType();
      if (logicalType != null && "beam:logical_type:date:v1".equals(logicalType.getIdentifier())) {
        if (val instanceof Integer) {
          return java.time.LocalDate.ofEpochDay((Integer) val);
        }
      }
    }
    if (type.getTypeName() == Schema.TypeName.DATETIME) {
      if (val instanceof Long) {
        return new org.joda.time.Instant(((Long) val) / 1000);
      }
      if (val instanceof java.time.LocalDateTime) {
        java.time.LocalDateTime ldt = (java.time.LocalDateTime) val;
        return new org.joda.time.Instant(
            ldt.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli());
      }
    }
    if (val instanceof org.apache.arrow.vector.util.Text) {
      return val.toString();
    }
    return val;
  }
}
