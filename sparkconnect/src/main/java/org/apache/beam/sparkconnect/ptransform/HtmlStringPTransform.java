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

public class HtmlStringPTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final Schema OUTPUT_SCHEMA =
      Schema.builder().addStringField("html_string").build();

  private final int numRows;
  private final int truncate;

  public HtmlStringPTransform(int numRows, int truncate) {
    this.numRows = numRows;
    this.truncate = truncate;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    Schema schema = input.getSchema();
    PCollection<Iterable<Row>> limitedInput = input.apply(Sample.fixedSizeGlobally(numRows));

    return limitedInput
        .apply(
            "FormatHtmlString",
            ParDo.of(
                new DoFn<Iterable<Row>, Row>() {
                  @ProcessElement
                  public void processElement(@Element Iterable<Row> rows, OutputReceiver<Row> out) {
                    String result = formatRowsToHtml(schema, ImmutableList.copyOf(rows), truncate);
                    out.output(Row.withSchema(OUTPUT_SCHEMA).addValue(result).build());
                  }
                }))
        .setRowSchema(OUTPUT_SCHEMA);
  }

  private String formatRowsToHtml(Schema schema, List<Row> rows, int truncate) {
    StringBuilder sb = new StringBuilder();
    sb.append("<table border=\"1\">\n");

    // Header
    sb.append("<thead>\n<tr>");
    for (String fieldName : schema.getFieldNames()) {
      sb.append("<th>").append(escapeHtml(fieldName)).append("</th>");
    }
    sb.append("</tr>\n</thead>\n");

    // Body
    sb.append("<tbody>\n");
    for (Row row : rows) {
      sb.append("<tr>");
      for (int i = 0; i < schema.getFieldCount(); i++) {
        String value = Objects.toString(row.getValue(i));
        if (truncate > 0 && value.length() > truncate) {
          value = value.substring(0, truncate - 3) + "...";
        }
        sb.append("<td>").append(escapeHtml(value)).append("</td>");
      }
      sb.append("</tr>\n");
    }
    sb.append("</tbody>\n");
    sb.append("</table>");

    return sb.toString();
  }

  private String escapeHtml(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;");
  }
}
