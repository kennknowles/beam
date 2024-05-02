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
package org.apache.beam.sdk.io.iceberg;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.beam.sdk.io.iceberg.SchemaAndRowConversions.rowToRecord;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@RunWith(JUnit4.class)
public class IcebergIOIT implements Serializable {

  public interface IcebergIOTestPipelineOptions extends PipelineOptions {
    @Description("Size of each record in bytes (for tests that vary record size)")
    @Default.Integer(100) // deliberately small so no-args execution is quick
    Integer getBytesPerRecord();

    void setBytesPerRecord(Integer bytesPerRecord);

    @Description("Number of records that will be written and/or read by the test")
    @Default.Integer(100) // deliberately small so no-args execution is quick
    Long getNumRecords();

    void setNumRecords(Long numRecords);

    @Description("Number of shards in the test table")
    @Default.Integer(10)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @Description("Location on blobstore for catalog files")
    String getCatalogLocation();

    void setCatalogLocation(String catalogLocation);
  }

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() {
    PipelineOptionsFactory.register(IcebergIOTestPipelineOptions.class);
  }

  /**
   * Test of a predetermined moderate number of records written directly to Iceberg then read via
   * a Beam pipeline. Table initialization is done on a single process using the Iceberg APIs so the
   * data cannot be "big".
   *
   * <p>The test itself presumes that checking exact results is impractical, so instead performs
   * look for a contiguous set of integers.
   */
  @Test
  public void testRead() throws Exception {
    IcebergIOTestPipelineOptions options = TestPipeline.testingPipelineOptions().as(IcebergIOTestPipelineOptions.class);

    Long numRecords = options.getNumRecords();
    Integer numShards = options.getNumShards();
    double recordsPerShardFraction = numRecords.doubleValue() / numShards.doubleValue();
    long maxRecordsPerShard = Math.round(Math.ceil(recordsPerShardFraction));

    Configuration catalogHadoopConf = new Configuration();
    String warehouseLocation =
        "gs://apache-beam-testing-integration-testing/IcebergIOIT/testRead/"
            + UUID.randomUUID().toString();
    Catalog catalog = new HadoopCatalog(catalogHadoopConf, warehouseLocation);

    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
        Types.NestedField.required(1, "v", Types.LongType.get()));
    Table table = catalog.createTable(tableId, icebergSchema);

    // Populate via laptop and remember the xor
    AppendFiles appendFiles = table.newAppend();

    int xorOfInputs = 0;
    int totalRecords = 0;
    for (int shardNum = 0; shardNum < options.getNumShards(); ++shardNum) {
      String filepath = table.location() + "/" + UUID.randomUUID().toString();
      OutputFile file = table.io().newOutputFile(filepath);
      DataWriter<Record> writer =
          Parquet.writeData(file)
              .schema(icebergSchema)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .withSpec(table.spec())
              .build();

      for (int recordNum = 0;
          recordNum < maxRecordsPerShard && totalRecords < options.getNumRecords();
          ++recordNum, ++totalRecords) {
        xorOfInputs = xorOfInputs ^ recordNum;
        writer.write(GenericRecord.create(icebergSchema).copy("v", recordNum));
      }
      writer.close();
      appendFiles.appendFile(writer.toDataFile());
    }
    appendFiles.commit();

    // Read with Dataflow
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setName("hadoop")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouseLocation)
            .build();

    PCollection<Integer> output =
        readPipeline
            .apply(IcebergIO.readRows(catalogConfig).from(tableId))
            .apply(MapElements.via(new SimpleFunction<Row, Integer>(r -> r.getInt32("v")) {}))
            .apply(Combine.globally(new SerializableBiFunction<Integer, Integer, Integer>() {
                                      @Override
                                      public Integer apply(Integer i, Integer j) {
                                        return i ^ j;
                                      }
                                    }));

    PAssert.that(output).containsInAnyOrder(xorOfInputs);

    readPipeline.run().waitUntilFinish();
  }
}
