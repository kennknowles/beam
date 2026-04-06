# Update Ignore List

**Purpose**: Manage which tests are bypassed during the Spark Connect compliance test run. When improving compliance, we automatically evaluate the test suite and ignore anything that fails, leaving only tests that pass.

**How to Use**:
When you fix a bug that unblocks new tests, or when you are assigned to update the ignore list, we do not selectively target individual lines to edit manually. Instead, we use a fully automated process to guarantee accuracy.

1. Navigate to the `sparkconnect/` directory in the `apache/beam-speak` repository.
2. Run the automated update script using your `run_command` tool:
   ```bash
   cd /usr/local/google/home/klk/GitHub/apache/beam-speak/sparkconnect && ./update_ignore_list.py
   ```

**What the Script Does**:
*   **Step 1**: It automatically runs the *entire* compliance test suite (bypassing `ignored_tests.txt`) to identify exactly what is failing in the current build.
*   **Step 2**: It parses the output, records all `FAILED` or `ERROR` tests, and automatically rewrites the `ignored_tests.txt` file to match this exact result.
*   **Step 3**: It runs the full compliance test suite again, this time utilizing the newly generated `ignored_tests.txt`, to verify that 100% of the non-ignored tests pass successfully and the ignore tests fail.

**Important Considerations**:
Because this script runs the test suite twice (once fully, once filtered), it may take several minutes to run. Please patiently wait out the task execution until the output confirms the verification status.
