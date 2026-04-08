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

import java.io.PrintWriter;
import java.lang.reflect.Field;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Test;

public class ListOperatorsTest {
  @Test
  public void testList() throws Exception {
    try (PrintWriter pw =
        new PrintWriter(
            "/usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect/operators.txt",
            "UTF-8")) {
      pw.println("--- SqlStdOperatorTable ---");
      listOperators(SqlStdOperatorTable.class, pw);
      pw.println("\n--- SqlLibraryOperators ---");
      listOperators(SqlLibraryOperators.class, pw);
    }
  }

  private void listOperators(Class<?> clazz, PrintWriter pw) {
    for (Field field : clazz.getFields()) {
      if (SqlOperator.class.isAssignableFrom(field.getType())) {
        try {
          SqlOperator op = (SqlOperator) field.get(null);
          if (field.getName().contains("BIT") || op.getName().contains("BIT")) {
            pw.println("OP_LIST: " + field.getName() + " : " + op.getName());
          }
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }
}
