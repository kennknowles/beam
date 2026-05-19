---
name: spark-connect-protobuf-context
description: Guides development for the Spark Connect server by ensuring the protobuf definitions are loaded into context.
---

# Spark Connect Development Guidelines - Protobuf Context

Whenever you are working on tasks related to the Spark Connect server in the `sparkconnect/` directory, you MUST follow these instructions to ensure you have the correct context:

1. **Reference Spark Connect Protobuf Definitions**: The Spark Connect protobuf files define the protocol messages. The total size of these files is around 150KB+, so **do NOT read all of them at once** unless specifically needed for a full-scope task.
   - Directory: `/usr/local/google/home/klk/GitHub/apache/spark_clone/sql/connect/common/src/main/protobuf/spark/connect/`
   - **Instruction**: Instead of reading all files, use search tools to find the specific message, enum, or relation you need (e.g., search for `message Filter` or `message Expression`) to identify the relevant file, and read only that file.
   - **File Descriptions**:
     - `base.proto`: Defines the `SparkConnectService` gRPC service and core Request/Response types.
     - `catalog.proto`: Defines interactions with the Spark catalog (databases, tables, functions).
     - `commands.proto`: Defines executable commands like writing data, creating views, or executing arbitrary code.
     - `common.proto`: Contains shared message types like `StorageLevel` and identifiers.
     - `example_plugins.proto`: Demonstrates how to extend Spark Connect with custom plugins.
     - `expressions.proto`: Defines all expression types (literals, unresolved attributes, function calls, window functions).
     - `ml.proto`: Defines machine learning components and operations.
     - `ml_common.proto`: Common utilities and messages for machine learning.
     - `pipelines.proto`: Defines machine learning pipeline structures.
     - `relations.proto`: Defines all logical operations on DataFrames (Read, Project, Filter, Join, Aggregate, etc.).
     - `types.proto`: Defines the Spark SQL data type representations (Integer, String, Struct, Array, etc.).

2. **Reference Spark Server Implementation**: The Spark codebase contains the server-side implementation of Spark Connect. You should refer to it to understand how Spark handles the protocol.
   - Directory: `/usr/local/google/home/klk/GitHub/apache/spark_clone/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/`
   - **Key Files and Summaries**:
     - `planner/SparkConnectPlanner.scala`: The core file that translates proto relations and expressions into Spark logical plans. **Most expression translation logic is here** (search for `def transformExpression`). It is very large (170KB+), so **do NOT read the entire file**.
     - `planner/LiteralExpressionProtoConverter.scala`: A specific helper file for translating proto literals into Spark expressions.
     - `execution/SparkConnectPlanExecution.scala`: Handles the actual execution of resolved Spark plans and manages streaming results back to the client. **This is the key file for Arrow translation** (look for `processAsArrowBatches`).
     - `service/SparkConnectService.scala`: The main gRPC service implementation that receives requests and routes them to handlers.
     - `service/SessionHolder.scala`: Manages the state and lifecycle of a Spark Connect session.
     - `service/ExecuteHolder.scala`: Manages the state of a specific execution request within a session.
     - `service/SparkConnectExecutePlanHandler.scala`: Handles the execution of plans by invoking the planner and execution engine.
     - `service/SparkConnectAnalyzeHandler.scala`: Handles analysis requests (e.g., getting schema, explain strings).
     - `ml/MLHandler.scala`: Handles machine learning extension commands and relations (training, inference, evaluation).
     - `ml/MLUtils.scala`: Utilities for machine learning plan transformations and model mapping.
     - `ml/MLCache.scala`: Server-side caching mechanism for loaded and fitted ML models.
     - `pipelines/PipelinesHandler.scala`: Handles ML pipeline definitions and execution workflows.
