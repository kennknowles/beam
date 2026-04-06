# Compute Compliance Stats

**Purpose**: Summarize the current compliance rate of the Apache Beam Spark Connect translation layer by comparing the total number of PySpark connect tests against those that are supported vs. currently ignored, broken down by test categories.

**How to Use**:
When you need to check the current compliance stats, execute the predefined Gradle task from root of the `apache/beam-speak` repository:

```bash
./gradlew :sparkconnect:computeComplianceStats
```

This task parses `ignored_tests.txt` for ignored tests, reads the total count from PySpark via `pytest --collect-only`, categorizes each test, and outputs a summary formatted like this:
```
=================================================
        COMPLIANCE STATUS SUMMARY                
=================================================
Total Connect Tests : 2920
Supported Tests     : 202
Ignored Tests       : 2718
Compliance Rate     : 6.92%
=================================================
        COMPLIANCE BY CATEGORY                   
=================================================
Category                       | Total  | Supp  | Ign  | Compliance %
-----------------------------------------------------------------
arrow                          | 717    | 0     | 717  |   0.00%
test_parity_udtf               | 403    | 0     | 403  |   0.00%
pandas                         | 366    | 4     | 362  |   1.09%
...
```

**Improving Compliance**:
If you are tasked with identifying what area to work on next, use the output of this script grouped by category to see where compliance is 0% or low. You can then consult the "identify next steps" or "update ignore list" skills.

**Important Rule for Reporting**:
Always give a complete report to the user, not just top categories. It is fine to also summarize top categories, but you must consistently provide the full, exhaustive list including the long tail of test categories to ensure that no compliance gaps are overlooked.
