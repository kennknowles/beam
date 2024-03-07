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
package org.apache.beam.sdk.providers;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.providers.GenerateSequenceSchemaTransformProvider.GenerateSequenceConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

@AutoService(SchemaTransformProvider.class)
public class ManagedSchemaTransformProvider
    extends TypedSchemaTransformProvider<ManagedTransformConfiguration> {
  public static final String OUTPUT_ROWS_TAG = "output";
  public static final Schema OUTPUT_SCHEMA = Schema.builder().addInt64Field("value").build();

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:generate_sequence:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ROWS_TAG);
  }

  @Override
  public String description() {
    return String.format(
        "Outputs a PCollection of Beam Rows, each containing a single INT64 "
            + "number called \"value\". The count is produced from the given \"start\" "
            + "value and either up to the given \"end\" or until 2^63 - 1.%n"
            + "To produce an unbounded PCollection, simply do not specify an \"end\" value. "
            + "Unbounded sequences can specify a \"rate\" for output elements.%n"
            + "In all cases, the sequence of numbers is generated in parallel, so there is no "
            + "inherent ordering between the generated values");
  }

  @Override
  public Class<GenerateSequenceConfiguration> configurationClass() {
    return GenerateSequenceConfiguration.class;
  }

  @Override
  public SchemaTransform from(GenerateSequenceConfiguration configuration) {
    return new GenerateSequenceSchemaTransform(configuration);
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ManagedTransformConfig {

    public static Builder builder() {
      return new AutoValue_ManagedSchemaTransformProvider_ManagedSchemaTransformProvider
          .Builder();
    }

    @SchemaFieldDescription("The URN of the underlying transform to instantiate.")
    public abstract String getUrn();

    @SchemaFieldDescription(
        "The configuration YAML for the transform, or null if the configuration is external.")
    public abstract @Nullable String getConfig();

    @SchemaFieldDescription(
        "The URL of the external configuration, or null if the configuration is inline.")
    public abstract @Nullable String getConfigUrl();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setUrn(String urn);

      public abstract Builder setConfig(@Nullable String config);

      public abstract Builder setConfigUrl(@Nullable String configUrl);

      public abstract ManagedTransformConfig build();
    }

    public void validate() {
      checkState(getConfigLocation() != null || getConfig() != null,
      "Either configLocation or config must be provided");
    }
  }

  protected static class ManagedSchemaTransform extends SchemaTransform {
    private final ManagedTransformConfig configuration;

    ManagedSchemaTransform(ManagedTransformConfig configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {

      String configContents = configuration.getConfig();
      if (configContents == null) {
        configContents =
            FileSystems.open(
                    FileSystems.matchSingleFileSpec(configuration.getConfigUrl()).resourceId())
                .toString();
      }

      Row configRow = yamlToRow(configContents);
      SchemaTransformProvider provider = registrar.getProvider(configuration.getUrn());
      SchemaTransform transform = provider.from(configRow);

      return input.apply(transform);
    }
  }
}
