# Identify Compliance Next Steps and Estimate Effort

**Purpose**: Guide the agent in evaluating the current state of conformance and strategically selecting the most efficient path forward for bug fixes.

**How to Use**:
Reference this skill when asked to plan out subsequent milestones or evaluate how hard it is to increase compliance in a specific area.

**Process Execution**:
1. **Data Gathering**: Run `./gradlew :sparkconnect:computeComplianceStats` from the root of the repository to view the current landscape of compliance deficits across categories.
2. **Category Selection**: Choose a category with 0% or particularly low compliance, preferably one that covers core SQL/DataFrame functionality (like `test_parity_functions` or `test_parity_types`).
3. **Poking the Tests**: Extract 1-3 typical tests for that category from `ignored_tests.txt`. Run them manually:
   ```bash
   ./gradlew :sparkconnect:complianceTests -PnoIgnoreList -PtestTarget="/usr/local/google/home/klk/GitHub/apache/spark_clone/python/pyspark/sql/tests/connect/test_parity_functions.py::FunctionsParityTests::test_specific_method"
   ```
4. **Failure Analysis & Effort Estimation**: Examine the output in `server.log` to categorize the error:
   *   **Parsing / SQL Translation (`ParseException`)**: Low effort. Requires adding regex replacements to `ExecutePlanHandler.java` and `SparkRelationToRelNode.java`.
   *   **Logical Planning (`CannotPlanException`)**: Medium effort. Usually involves addressing Calcite-specific quirks or missing mappings in `SparkExpressionToRexNode.java` or `CalciteUtils.java`.
   *   **Runtime Execution (`PipelineExecutionException` or Similar)**: High effort. Suggests core logic flaws in Beam SQL execution or unsupported PTransform nodes natively.
5. **Report Generation**: Synthesize your findings. Use an artifact or direct message to outline the failing stack trace, point out the source files needing work, and declare the expected effort.
