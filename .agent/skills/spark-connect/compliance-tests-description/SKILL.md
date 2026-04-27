---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name: compliance-tests-description
description: Guide on how to use the large sparkconnect/compliance_tests_description.md file to learn about compliance tests efficiently.
---

# Using Spark Compliance Tests Description

This skill provides guidance on how to efficiently use the `sparkconnect/compliance_tests_description.md` file. This file is very large (approx. 800KB) and contains descriptions of all tests in the Spark compliance suite. Loading the entire file into context is not recommended.

## 1. Searching for Tests

To learn about a specific compliance test, do not read the file sequentially. Instead:
*   **Action:** Search for the test by name using `grep_search` or by searching within a specific range if you have an idea where it is.
*   **Example:** Search for `test_udf` or a specific method name to find its description.

## 2. Reading in Small Chunks

Because the file is large, you should read it in manageable chunks.
*   **Manageable Chunk Size:** We recommend reading chunks of **100 to 200 lines** at a time.
*   **Tool:** Use `view_file` with `StartLine` and `EndLine` parameters to fetch only the relevant section.

## 3. Adding Surrounding Context

When you find a test and want to add its description to your context:
*   **Guidance:** It makes sense to add some surrounding tests as well to understand the context (e.g., other tests in the same class or file).
*   **Action:** When you locate a test, read a range that includes a few lines before and after the test definition to capture the class context and adjacent tests.

## 4. Referring to Full Test Source Code

The descriptions in `compliance_tests_description.md` are summaries.
*   **Guidance:** You may sometimes want to refer to the full text of the compliance test.
*   **Location:** The full text can be found in the Spark clone repository that we are using to run these tests against our Spark Connect server implementation.
*   **Default Path:** The Spark clone is typically located at `../spark_clone` relative to the workspace root (or as configured by the `SPARK_CLONE_DIR` environment variable in `sparkconnect/compliance_testing.py`).
*   **Paths within Spark Repo:** The tests are located in directories like:
    *   `python/pyspark/sql/tests/connect`
    *   `python/pyspark/ml/tests/connect`
    *   `python/pyspark/pandas/tests/connect`
    *   `python/pyspark/errors/tests/connect`
    *   `python/pyspark/logger/tests/connect`

## Best Practices

*   **Do not read the whole file:** It will overwhelm your context and waste tokens.
*   **Use Grep:** Always use `grep_search` first to find the line number of the test you are interested in.
*   **Read Chunks:** Once you have the line number, use `view_file` to read a small window around that line.
*   **Invoke via Gradle:** When running tests or updating the ignore list, always use the provided Gradle tasks (e.g., `./gradlew :sparkconnect:complianceTests`, `./gradlew :sparkconnect:updateIgnoreList`) instead of invoking `compliance_testing.py` directly. This ensures correct environment and dependency management.
