---
name: update-ignore-list
description: Guide on how to manage and update the Spark Connect compliance test ignore list.
---

# Update Ignore List

**Purpose**: Manage which tests are bypassed during the Spark Connect compliance test run. When improving compliance, we automatically evaluate the test suite and ignore anything that fails, leaving only tests that pass.

**Key File**: `sparkconnect/ignored_tests.txt`

## Format

The file contains one test identifier per line. It supports inline comments and tagging.

```text
# This is a comment line
python/pyspark/sql/tests/connect/test_parity_simple.py::SimpleTests::test_select
python/pyspark/sql/tests/connect/test_parity_flake.py::FlakyTests::test_unstable # flake
```

- **Comments**: Any text following a `#` symbol is treated as a comment and ignored.
- **Flake Tag**: Tests marked with ` # flake` are identified as flaky tests. These are preserved during updates even if they pass in a single run.
- **Alphabetical Order**: The file is maintained in precise alphabetical order by the automated script to ensure consistency and ease of comparison.

## How to Use / Update

When you fix a bug that unblocks new tests, or when you are assigned to update the ignore list, we do not selectively target individual lines to edit manually. Instead, we use a fully automated process to guarantee accuracy.

Always use the Gradle task from the root of the repository to update the ignore list to ensure correct environment and dependency management.

```bash
./gradlew :sparkconnect:updateIgnoreList
```

### What the Script Does

1. **Step 1**: It automatically runs the *entire* compliance test suite (bypassing `ignored_tests.txt`) to identify exactly what is failing in the current build.
2. **Step 2**: It parses the output, records all `FAILED` or `ERROR` tests.
3. It reads the existing `ignored_tests.txt` to identify tests marked as `# flake`.
4. It automatically rewrites the `ignored_tests.txt` file to match this exact result, containing the union of current failures and previously identified flaky tests.
5. Tests are only removed if they are neither failing nor marked as flaky.

## Important Considerations

*   **Check for Resource Constraints and Infrastructure Failures**: Before accepting a newly generated ignore list—especially if there is an unexpected spike in test failures—you MUST verify that the failures represent genuine code or compatibility gaps rather than temporary system resource constraints or infrastructure crashes.
    1. **Read Test Logs**: Always inspect the execution logs for failed tests (typically found in the task log or `sparkconnect/full_compliance_run.log`).
    2. **Chunked Log Reading**: Because test logs can be extremely large, start by reading just the last bit (e.g. using `tail` or reading the final chunk) to look for crash summaries or fatal errors. If the cause is not clear at the end, chunk backward through the log.
    3. **Identify Infrastructure Issues**: Look for signs such as fatal JVM/Python crashes (`Segmentation fault`, `Out of memory`), connection pool exhaustion, or testing harness timeouts.
    4. **Action**: If you see evidence that a bunch of tests failed due to testing infrastructure issues or resource limits, **do NOT update the ignore list**. Alert the user immediately with the relevant log snippets and restore the original ignore list.
*   **Skip Verification run**: We used to run the tests again to ensure we got it right. We don't need to do that. However, if the number of tests ignored or unignored is large, it is a good idea to alert the user and have them review the change. If a large number of tests have been regressed, it could mean that there was a failure unrelated to code changes (such as the whole test run failing due to a server issue). If a large number of tests have started passing, it could mean that we are accidentally skipping them but don't realize it.
*   **Time to Run**: Even without the verification run, running the full test suite in Step 1 may take several minutes. Please be patient.

## Verification

After updating the ignore list, you can check the changes using `git diff`:

```bash
git diff sparkconnect/ignored_tests.txt
```

This will show you which tests were added or removed.
