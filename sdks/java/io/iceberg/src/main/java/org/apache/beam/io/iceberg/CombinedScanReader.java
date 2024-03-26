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
package org.apache.beam.io.iceberg;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import javax.annotation.Nullable;
import org.apache.beam.io.iceberg.util.SchemaHelper;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

public class CombinedScanReader extends BoundedSource.BoundedReader<Row> {
  private static final Logger LOG = LoggerFactory.getLogger(CombinedScanReader.class);

  private final IcebergBoundedSource source;
  private final Table table;

  private final CombinedScanTask task;

  private final Schema schema;

  transient @Nullable org.apache.iceberg.Schema project;
  transient @Nullable FileIO io;
  transient @Nullable InputFilesDecryptor decryptor;
  transient @Nullable Queue<FileScanTask> files;
  transient @Nullable CloseableIterator<Record> baseIter;
  transient @Nullable Record current;

  public CombinedScanReader(
      IcebergBoundedSource source, CombinedScanTask task, Schema schema) {
    this.source = source;
    this.table = checkStateNotNull(source.table(),
        "CombinedScanReader requires an IcebergBoundedSource with a table");
    this.task = task;
    this.schema = schema;
    if (this.schema != null) {
      project = SchemaHelper.convert(this.schema);
    }
  }

  @Override
  public boolean start() throws IOException {
    if (task == null) {
      return false;
    }

    EncryptionManager encryptionManager = table.encryption();
    io = table.io();
    decryptor = new InputFilesDecryptor(task, io, encryptionManager);

    files = new ArrayDeque<>();
    files.addAll(task.files());

    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    Queue<FileScanTask> files = checkStateNotNull(this.files,
        "files null in advance() - did you call start()?");
    InputFilesDecryptor decryptor = checkStateNotNull(this.decryptor,
    "decryptor null in adance() - did you call start()?");

    // This is a lie, but the most expedient way to work with Iceberg's
    // which are not null-safe.
    @SuppressWarnings("nullness")
    org.apache.iceberg.@NonNull Schema project = this.project;

    do {
      // If our current iterator is working... do that.
      if (baseIter != null && baseIter.hasNext()) {
        current = baseIter.next();
        return true;
      }

      // Close out the current iterator and try to open a new one
      if (baseIter != null) {
        baseIter.close();
        baseIter = null;
      }

      LOG.info("Trying to open new file.");
      @Nullable FileScanTask fileTask = null;
      while (!files.isEmpty() && fileTask == null) {
        fileTask = files.remove();
        if (fileTask.isDataTask()) {
          LOG.error("{} is a DataTask. Skipping.", fileTask.toString());
          fileTask = null;
        }
      }

      // We have a new file to start reading
      if (fileTask != null) {
        DataFile file = fileTask.file();
        InputFile input = decryptor.getInputFile(fileTask);

        @Nullable CloseableIterable<Record> iterable = null;
        switch (file.format()) {
          case ORC:
            LOG.info("Preparing ORC input");
            iterable =
                ORC.read(input)
                    .project(project)
                    .createReaderFunc(
                        fileSchema -> GenericOrcReader.buildReader(project, fileSchema))
                    .filter(fileTask.residual())
                    .build();
            break;
          case PARQUET:
            LOG.info("Preparing Parquet input.");
            iterable =
                Parquet.read(input)
                    .project(project)
                    .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(project, fileSchema))
                    .filter(fileTask.residual())
                    .build();
            break;
          case AVRO:
            LOG.info("Preparing Avro input.");
            iterable =
                Avro.read(input).project(project).createReaderFunc(DataReader::create).build();
            break;
          default:
            throw new UnsupportedOperationException("Cannot read format: " + file.format());
        }

        if (iterable != null) {
          baseIter = iterable.iterator();
        }
      } else {
        LOG.info("We have exhausted all available files in this CombinedScanTask");
      }

    } while (baseIter != null);
    return false;
  }

  private Row convert(Record record) {
    Row.Builder b = Row.withSchema(schema);
    for (int i = 0; i < schema.getFieldCount(); i++) {
      // TODO: A lot obviously
      b.addValue(record.getField(schema.getField(i).getName()));
    }
    return b.build();
  }

  @Override
  public Row getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return convert(current);
  }

  @Override
  public void close() throws IOException {
    if (baseIter != null) {
      baseIter.close();
    }
    if (files != null) {
      files.clear();
      files = null;
    }
    if (io != null) {
      io.close();
    }
  }

  @Override
  public BoundedSource<Row> getCurrentSource() {
    return source;
  }
}
