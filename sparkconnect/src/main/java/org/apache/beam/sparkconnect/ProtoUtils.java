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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Utility for printing protobuf messages while truncating large data fields like Arrow batches. */
public class ProtoUtils {

  public static String debugString(MessageOrBuilder message) {
    if (message == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    printMessage(message, 0, sb);
    return sb.toString();
  }

  private static void printMessage(MessageOrBuilder message, int indent, StringBuilder sb) {
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      Descriptors.FieldDescriptor field = entry.getKey();
      Object value = entry.getValue();

      if (field.isRepeated()) {
        List<?> elements = (List<?>) value;
        for (Object element : elements) {
          if (element != null) {
            printField(field, element, indent, sb);
          }
        }
      } else {
        printField(field, value, indent, sb);
      }
    }
  }

  private static void printField(
      Descriptors.FieldDescriptor field, Object value, int indent, StringBuilder sb) {
    String indentStr = String.join("", Collections.nCopies(indent, "  "));
    sb.append(indentStr).append(field.getName());

    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
      sb.append(" {\n");
      printMessage((Message) value, indent + 1, sb);
      sb.append(indentStr).append("}\n");
    } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.BYTE_STRING) {
      ByteString bytes = (ByteString) value;
      if (bytes.size() > 100) { // arbitrary threshold to clip Arrow batch / LocalRelation schemas
        sb.append(": <ByteString size=").append(bytes.size()).append(" bytes>\n");
      } else {
        sb.append(": \"").append(TextFormat.escapeBytes(bytes)).append("\"\n");
      }
    } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
      sb.append(": \"")
          .append(TextFormat.escapeBytes(ByteString.copyFromUtf8((String) value)))
          .append("\"\n");
    } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
      sb.append(": ").append(((Descriptors.EnumValueDescriptor) value).getName()).append("\n");
    } else {
      sb.append(": ").append(value.toString()).append("\n");
    }
  }
}
