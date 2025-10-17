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
package org.apache.beam.sparkconnect.ptransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class ShowStringPTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final Schema OUTPUT_SCHEMA =
      Schema.builder().addStringField("show_string").build();

  private final int numRows;
  private final int truncate;
  private final boolean vertical;

  public ShowStringPTransform(int numRows, int truncate, boolean vertical) {
    this.numRows = numRows;
    this.truncate = truncate;
    this.vertical = vertical;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    Schema schema = input.getSchema();
    PCollection<Iterable<Row>> limitedInput = input.apply(Sample.fixedSizeGlobally(numRows));

    return limitedInput
        .apply(
            "FormatShowString",
            ParDo.of(
                new DoFn<Iterable<Row>, Row>() {
                  @ProcessElement
                  public void processElement(@Element Iterable<Row> rows, OutputReceiver<Row> out) {
                    String result =
                        formatRows(schema, ImmutableList.copyOf(rows), numRows, truncate, vertical);
                    out.output(Row.withSchema(OUTPUT_SCHEMA).addValue(result).build());
                  }
                }))
        .setRowSchema(OUTPUT_SCHEMA);
  }

  /**
   * This method contains the core logic for formatting the rows into a string, similar to Spark's
   * showString.
   */
  private String formatRows(
      Schema schema, List<Row> rows, int numRows, int truncate, boolean vertical) {

    // Assemble rows, including headers
    List<List<String>> stringRows = new ArrayList<>();
    stringRows.add(schema.getFieldNames());
    for (Row r : rows) {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      for (int i = 0; i < schema.getFieldCount(); ++i) {
        builder.add(Objects.toString(r.getValue(i)));
      }
      stringRows.add(builder.build());
    }

    // Calculate column widths
    int[] widths = new int[schema.getFieldCount()];
    for (List<String> rowData : stringRows) {
      for (int i = 0; i < rowData.size(); i++) {
        widths[i] = Math.max(widths[i], rowData.get(i).length());
      }
    }

    if (truncate > 0) {
      for (int i = 0; i < widths.length; i++) {
        widths[i] = Math.min(widths[i], truncate);
      }
    }

    // Header looks like this:
    // +--------+----------+-------+
    // | field1 |  field2  |   x   |
    // +--------+----------+-------+
    StringBuilder sb = new StringBuilder();
    appendSeparator(sb, widths);
    appendRow(sb, schema.getFieldNames(), widths, truncate);
    appendSeparator(sb, widths);
    for (List<String> rowData : stringRows.subList(1, stringRows.size())) {
      appendRow(sb, rowData, widths, truncate);
    }
    appendSeparator(sb, widths);

    return sb.toString();
  }

  private void appendSeparator(StringBuilder sb, int[] widths) {
    sb.append("+");
    for (int width : widths) {
      for (int i = 0; i < width + 2; i++) {
        sb.append("-");
      }
      sb.append("+");
    }
    sb.append("\n");
  }

  private void appendRow(StringBuilder sb, List<String> row, int[] widths, int truncate) {
    sb.append("|");
    for (int i = 0; i < row.size(); i++) {
      String value = row.get(i);
      if (truncate > 0 && value.length() > truncate) {
        value = value.substring(0, truncate - 3) + "...";
      }
      sb.append(" ");
      sb.append(String.format("%-" + widths[i] + "s", value));
      sb.append(" |");
    }
    sb.append("\n");
  }
}
