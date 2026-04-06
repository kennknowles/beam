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

**Important Considerations**:
*   **Skip Verification run**: We used to run the tests again to ensure we got it right. We don't need to do that. However, if the number of tests ignored or unignored is large, it is a good idea to alert the user and have them review the change. If a large number of tests have been regressed, it could mean that there was a failure unrelated to code changes (such as the whole test run failing due to a server issue). If a large number of tests have started passing, it could mean that we are accidentally skipping them but don't realize it.
*   **Time to Run**: Even without the verification run, running the full test suite in Step 1 may take several minutes. Please be patient.

