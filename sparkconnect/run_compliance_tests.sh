#!/usr/bin/env bash
# run_compliance_tests.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
SPARK_CLONE_DIR="/usr/local/google/home/klk/GitHub/apache/spark_clone"

if [ ! -d "${SPARK_CLONE_DIR}" ]; then
  echo "Spark clone not found at ${SPARK_CLONE_DIR}. Please clone it first:"
  echo "git clone --depth 1 https://github.com/apache/spark.git ${SPARK_CLONE_DIR}"
  exit 1
fi

VENV_DIR="${SCRIPT_DIR}/build/venv"
if [ ! -d "${VENV_DIR}" ]; then
  echo "Creating Python virtual environment..."
  python3 -m venv "${VENV_DIR}"
  source "${VENV_DIR}/bin/activate"
  echo "Installing requirements..."
  pip install --quiet --upgrade pip --index-url=https://pypi.org/simple/
  pip install --quiet pytest pyspark pandas pyarrow grpcio grpcio-status py4j googleapis-common-protos zstandard --index-url=https://pypi.org/simple/
else
  source "${VENV_DIR}/bin/activate"
fi
if [ -z "$JAVA_HOME" ]; then
  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
fi
export PATH="$JAVA_HOME/bin:$PATH"

echo "Starting blockingServer in background via Gradle..."
(cd "${SCRIPT_DIR}/.." && ./gradlew :sparkconnect:blockingServer -Dorg.gradle.java.home=$JAVA_HOME > "${SCRIPT_DIR}/server.log" 2>&1) &
SERVER_PID=$!

function cleanup {
  echo "Tearing down blockingServer (via Gradle stop or PID $SERVER_PID)..."
  # Try stopping gradle daemon nicely if it hangs
  pkill -f "org.apache.beam.sparkconnect.SparkConnectServer" || true
  kill $SERVER_PID 2>/dev/null || true
  wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "Waiting for server to start on port 12345..."
# Wait for port to open
SERVER_UP=0
for i in {1..900}; do
  if nc -z localhost 12345 2>/dev/null; then
    echo "Server is up!"
    SERVER_UP=1
    break
  fi
  sleep 1
done

if [ "$SERVER_UP" -eq 0 ]; then
  echo "Error: Server failed to start on port 12345 within 900 seconds. Aborting."
  exit 1
fi

echo "Running compliance tests..."
export SPARK_CONNECT_TESTING_REMOTE="sc://localhost:12345"
export SPARK_TESTING=1
# export SPARK_SKIP_CONNECT_COMPAT_TESTS=1
export SPARK_HOME="${SPARK_CLONE_DIR}"
TEST_DIR="${SPARK_CLONE_DIR}/python/pyspark/sql/tests/connect"

# Default to all tests if no arguments are provided:
if [ $# -gt 0 ]; then
  TEST_TARGET=("$@")
else
  TEST_TARGET=("${TEST_DIR}")
fi

IGNORED_TESTS_FILE="${SCRIPT_DIR}/ignored_tests.txt"
PYTEST_ARGS=()
if [ -f "$IGNORED_TESTS_FILE" ]; then
  echo "Reading ignored tests from $IGNORED_TESTS_FILE"
  while IFS= read -r line || [ -n "$line" ]; do
    if [[ -n "$line" && ! "$line" =~ ^# ]]; then
      PYTEST_ARGS+=("--deselect=$line")
    fi
  done < "$IGNORED_TESTS_FILE"
fi

# Diagnostics
echo "Checking PySpark requirements..."
python3 -c 'from pyspark.testing.utils import should_test_connect, connect_requirement_message; print(f"should_test_connect: {should_test_connect}"); print(f"connect_requirement_message: {connect_requirement_message}")'

# test_session.py causes a hang, so we continue to ignore it entirely.
pytest -v "${TEST_TARGET[@]}" "${PYTEST_ARGS[@]}" --ignore="${TEST_DIR}/test_session.py"

echo "Tests completed."
