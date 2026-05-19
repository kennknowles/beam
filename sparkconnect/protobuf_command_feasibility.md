<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Spark Connect Protobuf Command Feasibility Breakdown

This document outlines the difficulty level for implementing Spark Connect commands on the Apache Beam Spark Connect Server, categorized according to the [Spark Connect Protobuf protocol](file:///usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect/src/main/java/org/apache/beam/sparkconnect/SparkConnectService.java).

---

## `ExecutePlanRequest` Operations

Incoming execution commands dictate the generation of data or trigger operations on analytical pipelines.

### **Easy to Implement**
* **`ML_COMMAND`**
  * **Complexity**: **Easy**
  * **Reasoning**: PySpark issues these commands primarily for client-side machine learning cache cleanup. Because the Beam server doesn't hold a persistent remote ML cache, these can safely remain no-ops.

### **Medium Complexity**
* **`WRITE_OPERATION`**
  * **Complexity**: **Medium**
  * **Reasoning**: Batch writing DataFrame outputs to external sinks. Parquet and CSV formats are already partially supported via Beam's `ParquetIO` and `CsvIO`. Adding new formats (e.g., JSON, ORC) mostly requires standard mapping onto Beam IO sinks.
* **`CREATE_DATAFRAME_VIEW`**
  * **Complexity**: **Medium**
  * **Reasoning**: Requires registering the result of a PCollection translation as a named table within the session's catalog for Beam SQL.

### **High Complexity / Hard**
* **`ROOT` (Relation Plan)**
  * **Complexity**: **Hard / XL**
  * **Reasoning**: This is the primary execution path. It requires converting Spark's arbitrary logical plan tree (joins, filters, UDFs) into a relational node (`RelNode`) within Apache Calcite, and then running that plan on a Beam runner. Relational semantics between Spark and Calcite do not match 1:1.
* **`SQL_COMMAND`**
  * **Complexity**: **Hard / XL**
  * **Reasoning**: Evaluates a raw SQL query string. Spark SQL syntax differs significantly from Calcite's ANSI SQL parser. Currently requires extensive regular expression pre-processing of the SQL string to coerce syntax before parsing.
* **`REGISTER_FUNCTION`**
  * **Complexity**: **Hard**
  * **Reasoning**: Enables dynamic User-Defined Functions (UDFs). Implementing this involves generating custom Calcite `SqlFunction` objects at runtime and dynamically registering them within the query planner.
* **`WRITE_STREAM_OPERATION_START`**
  * **Complexity**: **Hard**
  * **Reasoning**: Translates relational streaming operations into continuous pipeline sinks. Requires careful orchestration of Beam's streaming triggering and windowing.

---

## `AnalyzePlanRequest` Operations

Analytical queries request metadata about logical plans without immediately generating execution workloads.

### **Easy to Implement**
* **`SCHEMA`**
  * **Complexity**: **Easy**
  * **Reasoning**: Already implemented. It pulls the relational schema from Calcite translation and translates `RelDataType` back to Spark `DataType`.
* **`SPARK_VERSION`** & **`IS_LOCAL`**
  * **Complexity**: **Easy**
  * **Reasoning**: Return static server configurations (e.g., version strings and locality flags). Already fully implemented.
* **`DDL_PARSE`**
  * **Complexity**: **Easy**
  * **Reasoning**: Translates a raw DDL string into a Spark DataType hierarchy. A functional parser already exists within `AnalyzePlanHandler.java`.

### **Medium Complexity**
* **`PERSIST`, `UNPERSIST`, & `GET_STORAGE_LEVEL`**
  * **Complexity**: **Medium**
  * **Reasoning**: Handles caching hints for Spark relations. Can be implemented by mapping Relation IDs to Beam's `.cache()` or `Materialize` steps, or simply maintaining a state map in memory.
* **`EXPLAIN`**
  * **Complexity**: **Medium**
  * **Reasoning**: Currently returns a dummy physical plan string. Can be fleshed out by dumping Calcite's RelNode tree text description via `RelOptUtil.toString()`.
* **`IS_STREAMING`**
  * **Complexity**: **Medium**
  * **Reasoning**: Inspects the boundedness of the translated PCollections.
* **`JSON_TO_DDL`**
  * **Complexity**: **Medium**
  * **Reasoning**: Standard translation from Spark's JSON schema representation to an inline DDL string.

### **High Complexity / Hard**
* **`SAME_SEMANTICS`**
  * **Complexity**: **Hard / XL**
  * **Reasoning**: Determines if two separate Spark Connect relations yield the exact same results. Requires deep inspection of Calcite RelNode subtrees or semantic graph hashing.
* **`SEMANTIC_HASH`**
  * **Complexity**: **Hard / XL**
  * **Reasoning**: Builds an identical structural hash for logical Spark plans. Requires normalizing relational plans inside Calcite before hashing.

---

## Machine Learning (MLlib) Extensions

MLlib operations are implemented via protobuf extensions wrapped inside generic `google.protobuf.Any` messages in both relations and commands.

### **Medium Complexity**
* **`Fetch` / `Summarizer`**
  * **Complexity**: **Medium**
  * **Reasoning**: Extracting summary statistics or evaluation metrics. Can be achieved by compiling an aggregation pipeline and packing the scalar output metrics back into ML extension result protos.

### **High Complexity / Hard**
* **`Transform` / `ModelTransform`**
  * **Complexity**: **Hard / XL**
  * **Reasoning**: Applying a loaded or fitted ML model to an input DataFrame. Implementing this requires unpacking model parameters and mapping them to Beam-native execution steps (e.g., using Beam's `RunInference` transform or custom mapping `PTransform`s).
* **`Write` / `Save`** & **`Read` / `Load`**
  * **Complexity**: **Hard**
  * **Reasoning**: Persisting and loading ML estimators or pipeline models. Requires bridging Spark's model serialization layout to a persistent format usable across distributed Beam workers.

---

## Pipeline Sub-Language Operations

The ML pipeline sub-language structures complex end-to-end machine learning workflows.

### **High Complexity / Hard**
* **`Pipeline` / `Fit`**
  * **Complexity**: **Hard / XL**
  * **Reasoning**: Training a chained sequence of ML estimators and transformers. Requires building a directed acyclic graph (DAG) of stage transformations, orchestrating intermediate state/schemas, and executing distributed training algorithms across Beam workers.
* **`CrossValidator` / `TrainValidationSplit`**
  * **Complexity**: **Hard / XL**
  * **Reasoning**: Model tuning and hyperparameter search workflows. Requires launching multiple alternative sub-pipelines concurrently and aggregating evaluation metrics to select the optimal model configuration.

---

## Specialized Streaming Commands

Streaming commands control real-time lifecycle management and query observation.

### **Medium Complexity**
* **`StreamingQueryManagerCommand` (List, AwaitAnyTermination)**
  * **Complexity**: **Medium**
  * **Reasoning**: Session-level management of active streams. Can be implemented by tracking active execution handles and status threads on the server side.
* **`StreamingQueryCommand` (Stop, Status, RecentProgress)**
  * **Complexity**: **Medium**
  * **Reasoning**: Interrogating or terminating a specific active streaming query. Requires hooking into Beam's job metrics or runner-specific APIs to expose progress updates in Spark's expected `StreamingQueryProgress` JSON format.

### **High Complexity / Hard**
* **`WRITE_STREAM_OPERATION_START`**
  * **Complexity**: **Hard / XL**
  * **Reasoning**: Launching an unbounded streaming pipeline to a target sink. Requires careful mapping of Spark's output modes (Append, Complete, Update) and triggering policies to Beam's windowing, triggers, and state accumulation semantics.

---

## Reference Source Files
- [SparkConnectService.java](file:///usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect/src/main/java/org/apache/beam/sparkconnect/SparkConnectService.java)
- [ExecutePlanHandler.java](file:///usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect/src/main/java/org/apache/beam/sparkconnect/handler/ExecutePlanHandler.java)
- [AnalyzePlanHandler.java](file:///usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect/src/main/java/org/apache/beam/sparkconnect/handler/AnalyzePlanHandler.java)
