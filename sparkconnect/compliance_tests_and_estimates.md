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

# Beam Spark Connect Compliance Tests & Estimates

This report presents the compliance design and effort estimates for achieving full compatibility across all categories and subcategories of Spark Connect tests, based on the April 2026 ignore list.

> [!NOTE]
> The compliance rate dropped to 24.39% following an automated test suite run and subsequent ignore list update, suggesting that newly introduced tests are currently failing or previously unignored tests are flaky.

---

## Current Compliance Status

- **Total Connect Tests**: 5182
- **Supported Tests**: 1264
- **Ignored Tests**: 3918
- **Current Compliance Rate**: **24.39%**

---

## Sizing and Feasibility Scales

We use a dual-axis measurement system consisting of **Effort T-Shirt Sizing** and **AI Ease Feasibility**:

### Effort Sizing
- **S**: Small — Minor fixes or simple translations already underway.
- **M**: Medium — Focused work needed to support a set of missing features.
- **L**: Large — Significant feature gaps requiring extensive implementation effort.
- **XL**: Extra Large — Major architectural gaps requiring fundamental Calcite/Beam translations.

### AI Feasibility
- **High**: The AI can likely resolve these with minimal human guidance (e.g., pattern matching).
- **Medium**: The AI can complete the work if provided with explicit patterns or similar translated examples.
- **Low**: The task requires deep relational mapping logic, complex Calcite rules, or human architectural oversight.

---

## Core Test Categories

| Category | Compliance (%) | Effort Sizing | AI Feasibility | Scope & Implementation Notes |
| :--- | :--- | :--- | :--- | :--- |
| **errors** | 42.86% | **S/M** | **High** | Fix traceback extraction for custom data sources and UDTFs. |
| **logger** | 63.64% | **S** | **High** | Add query context to logs and address flaky `log info` exceptions. |
| **ml** | 22.48% | **L** | **Medium** | Implement translations for specific ML algorithm parameters and caching. |
| **pandas** | 31.72% | **XL** | **Medium** | Address missing methods in the Pandas API on Spark translation layer. |
| **sql (Base)** | 32.61% | **XL** | **Low** | Core relational translations involving Calcite planning and complex Arrow types. |
| **sql/streaming** | 20.63% | **L** | **Low** | Implementation of missing relational plans for Structured Streaming. |

---

## Evaluation of Workload Styles

Enabling specific analytical workloads requires implementing cohesive blocks of functionality:

### 1. Core Batch Analytics & BI
- **Features**: Core DataFrame API, SQL functions, and Parquet I/O.
- **Categories**: `sql` (Base), `sql/parquet`.
- **Combined Effort**: **XL** (Dominated by core SQL gaps).
- **AI Feasibility**: **Low**.

### 2. Streaming Data Pipelines
- **Features**: Structured Streaming and source/sink connectors.
- **Categories**: `sql/streaming` and `sql` (Base).
- **Combined Effort**: **XL**.
- **AI Feasibility**: **Low**.

### 3. Data Science & ML Preparation
- **Features**: Pandas API on Spark, ML algorithms.
- **Categories**: `pandas` (Base), `ml`, and `sql` (Base).
- **Combined Effort**: **XL** (Dependent on the Core `sql` foundation).

---

## Detailed Subcategory Breakdown

### Pandas Subcategories
- **`plot`**: 87.14% compliance — **S** Effort, **High** AI Ease
- **`resample`**: 55.56% compliance — **S** Effort, **Medium** AI Ease
- **`reshape`**: 45.45% compliance — **S/M** Effort, **Medium** AI Ease
- **`window`**: 37.50% compliance — **M** Effort, **Medium** AI Ease
- **`io`**: 36.07% compliance — **M** Effort, **Medium** AI Ease
- **`groupby`**: 30.94% compliance — **M** Effort, **Medium** AI Ease
- **`indexes`**: 25.46% compliance — **L** Effort, **Medium** AI Ease
- **`series`**: 14.12% compliance — **L** Effort, **Medium** AI Ease
- **`frame`**: 3.70% compliance — **L** Effort, **Medium** AI Ease
- **`data_type_ops`**: 0.43% compliance — **XL** Effort, **Medium** AI Ease
- **`computation`**: 0.00% compliance — **XL** Effort, **Medium** AI Ease
- **`diff_frames_ops`**: 0.00% compliance — **L** Effort, **Medium** AI Ease

### SQL Subcategories
- **`client`**: 91.55% compliance — **S** Effort, **High** AI Ease
- **`shell`**: 77.78% compliance — **S** Effort, **High** AI Ease
- **`pandas`**: 20.96% compliance — **L** Effort, **Medium** AI Ease
- **`streaming`**: 20.63% compliance — **L** Effort, **Low** AI Ease
- **`pandas/streaming`**: 20.21% compliance — **M/L** Effort, **Low** AI Ease
- **`arrow`**: 19.28% compliance — **XL** Effort, **Low** AI Ease

---

## Reference Documents

- Raw Design and Estimates: [compliance_design_and_estimates.md](file:///usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect/compliance_design_and_estimates.md)
- Raw Compliance Test Descriptions: [compliance_tests_description.md](file:///usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect/compliance_tests_description.md)
- Ignored Test Suite: [ignored_tests.txt](file:///usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect/ignored_tests.txt)
