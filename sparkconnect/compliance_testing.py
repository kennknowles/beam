#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-;
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import subprocess
import time
import socket
import argparse
import shlex
import re
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SPARK_CLONE_DIR = os.environ.get("SPARK_CLONE_DIR", os.path.join(os.path.dirname(SCRIPT_DIR), "..", "spark_clone"))
SPARK_DOWNLOAD_DIR = os.environ.get("SPARK_DOWNLOAD_DIR", os.path.join(os.path.dirname(SPARK_CLONE_DIR), "spark-4.1.1-bin-hadoop3"))
IGNORED_TESTS_FILE = os.path.join(SCRIPT_DIR, "ignored_tests.txt")
TEST_DIR = os.path.join(SPARK_CLONE_DIR, "python", "pyspark", "sql", "tests", "connect")
TARGET_DIRS = [
    os.path.join("python", "pyspark", "sql", "tests", "connect"),
    os.path.join("python", "pyspark", "ml", "tests", "connect"),
    os.path.join("python", "pyspark", "pandas", "tests", "connect"),
    os.path.join("python", "pyspark", "errors", "tests", "connect"),
    os.path.join("python", "pyspark", "logger", "tests", "connect"),
    os.path.relpath(os.path.join(SCRIPT_DIR, "local_tests", "python", "pyspark", "sql", "tests", "connect"), SPARK_CLONE_DIR),
    os.path.relpath(os.path.join(SCRIPT_DIR, "local_tests", "python", "pyspark", "ml", "tests", "connect"), SPARK_CLONE_DIR)
]
VENV_DIR = os.path.join(SCRIPT_DIR, "build", "venv")

def get_python_exec():
    venv_python = os.path.join(VENV_DIR, "bin", "python")
    return venv_python if os.path.exists(venv_python) else "python3"

def ensure_venv():
    """Sets up standard testing environment and installs required python packages."""
    if not os.path.exists(VENV_DIR):
        print(f"Creating Python virtual environment at {VENV_DIR}...")
        subprocess.run(["python3", "-m", "venv", "--without-pip", VENV_DIR], check=True)
        print("Installing pip manually...")
        venv_python = os.path.join(VENV_DIR, "bin", "python")
        try:
            subprocess.run(["curl", "-sS", "https://bootstrap.pypa.io/get-pip.py", "-o", "get-pip.py"], check=True)
            subprocess.run([venv_python, "get-pip.py"], check=True)
            os.remove("get-pip.py")
        except Exception as e:
            print(f"Failed to install pip via get-pip.py: {e}")
            print("Falling back to using host pip to install into venv...")
            subprocess.run(["python3", "-m", "pip", "install", "--target", os.path.join(VENV_DIR, "lib", "python3.11", "site-packages"), "pip"], check=True)

    pip_exec = os.path.join(VENV_DIR, "bin", "pip")
    print("Installing requirements into virtual environment...")
    cmd = [
        pip_exec, "install", "--quiet", "--upgrade", "pip",
        "--index-url=https://pypi.org/simple/"
    ]
    subprocess.run(cmd, check=True)

    cmd = [
        pip_exec, "install", "--quiet", "--upgrade",
        "pytest", "pandas<3.0.0", "pyarrow", "grpcio", "grpcio-status",
        "py4j", "googleapis-common-protos", "zstandard", "pytest-timeout", "pytest-xdist",
        "--index-url=https://pypi.org/simple/"
    ]
    subprocess.run(cmd, check=True)

    print("Ensuring pyspark is not installed in the virtual environment...")
    cmd = [pip_exec, "uninstall", "-y", "pyspark"]
    subprocess.run(cmd, check=False)

def ensure_spark_clone():
    """Ensures the Spark clone exists, is fetched, and is hard reset."""
    print(f"Ensuring Spark clone at {SPARK_CLONE_DIR}...")
    
    if not os.path.exists(SPARK_CLONE_DIR):
        print(f"Spark clone not found. Cloning from https://github.com/apache/spark.git ...")
        subprocess.run(["git", "clone", "https://github.com/apache/spark.git", SPARK_CLONE_DIR], check=True)
    else:
        print("Spark clone exists. Fetching updates...")
        subprocess.run(["git", "fetch"], cwd=SPARK_CLONE_DIR)
        
    print("Performing hard reset to origin/master...")
    subprocess.run(["git", "reset", "--hard", "origin/master"], cwd=SPARK_CLONE_DIR, check=True)

def wait_for_port(port, host='localhost', timeout=900, process=None):
    """Wait until a port starts accepting TCP connections."""
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            if process and process.poll() is not None:
                raise RuntimeError(f"Process died with exit code {process.poll()} while waiting for port.")
            time.sleep(1)
            if time.time() - start_time > timeout:
                return False

class BlockingServerManager:
    """Context manager to run the blockingServer via Gradle in the background."""
    def __init__(self, port=12345):
        self.port = port
        self.java_home = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
        self.process = None

    def __enter__(self):
        print(f"Starting blockingServer in background via Gradle (JAVA_HOME={self.java_home})...")
        env = os.environ.copy()
        env["JAVA_HOME"] = self.java_home
        env["PATH"] = os.path.join(self.java_home, "bin") + os.pathsep + env.get("PATH", "")
        gradlew = os.path.join(SCRIPT_DIR, "..", "gradlew")
        
        # We use Popen and keep the reference to terminate it later.
        log_file = open(os.path.join(SCRIPT_DIR, "server.log"), "a")
        self.process = subprocess.Popen(
            [gradlew, ":sparkconnect:blockingServer", f"-Dorg.gradle.java.home={self.java_home}"],
            cwd=os.path.join(SCRIPT_DIR, ".."),
            stdout=log_file,
            stderr=log_file,
            env=env,
            preexec_fn=os.setsid  # Creates process group so we can forcefully kill gradle children
        )

        print(f"Waiting for server to start on port {self.port}...")
        if not wait_for_port(self.port, process=self.process):
            self._kill_process_group()
            raise RuntimeError(f"Server failed to start on port {self.port} within timeout.")
        print("Server is up!")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("Tearing down blockingServer...")
        self._kill_process_group()
        
    def _kill_process_group(self):
        if self.process:
            try:
                # Kill process gently to allow log flushing
                self.process.terminate()
            except OSError:
                pass
            finally:
                self.process.wait()
        
        # Also clean up lingering processes just in case
        subprocess.run(["pkill", "-f", "org.apache.beam.sparkconnect.SparkConnectServer"], capture_output=True)

def load_pytest_ignore_args(ignore_file=IGNORED_TESTS_FILE):
    """Reads the ignore list and compiles --deselect arguments."""
    args = []
    if os.path.exists(ignore_file):
        with open(ignore_file, "r") as f:
            for line in f:
                line = line.strip()
                if "#" in line:
                    line = line.split("#")[0].strip()
                if line:
                    args.append(f"--deselect={line}")
    return args

def extract_category(test_identifier):
    """
    Extracts the category and subcategory of the test from the file path.
    Returns a tuple: (category, subcategory)
    """
    parts = test_identifier.split("::")
    filepath = parts[0]
    
    category = "Unknown"
    subcategory = ""
    
    # Extract category (module name)
    match = re.search(r"pyspark/([^/]+)/tests/connect", filepath)
    if match:
        category = match.group(1)
        
    # Extract subcategory (path within tests/connect)
    match_sub = re.search(r"tests/connect/(.+)", filepath)
    if match_sub:
        rel_path = match_sub.group(1)
        path_parts = rel_path.split("/")
        if len(path_parts) > 1:
            # It's in a subdirectory
            subcategory = "/".join(path_parts[:-1])
        else:
            # It's directly in tests/connect
            subcategory = ""
            
    return category, subcategory


def do_run(args):
    """Executes the Pytest suite utilizing the ignore list."""
    ensure_venv()
    # ensure_spark_clone()
    
    with BlockingServerManager():
        print("Running compliance tests...")
        env = os.environ.copy()
        env["SPARK_CONNECT_TESTING_REMOTE"] = "sc://localhost:12345"
        env["SPARK_TESTING"] = "1"
        env["SPARK_HOME"] = SPARK_DOWNLOAD_DIR
        env["PYTHONPATH"] = os.path.join(SPARK_CLONE_DIR, "python") + (os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else "")
        env["SPARK_SKIP_CONNECT_COMPAT_TESTS"] = "1"
        
        if args.profile and not args.test_targets:
            print("Error: You must specify specific test targets when profiling to avoid running the entire suite.")
            sys.exit(1)
            
        test_targets = args.test_targets if args.test_targets else TARGET_DIRS
        
        if args.profile:
            print("Profiling enabled. Disabling parallel execution.")
            pytest_args = [
                get_python_exec(), "-m", "cProfile", "-s", "cumulative",
                "-m", "pytest", "-v",
                "--timeout=100", "--durations=100",
                f"--ignore={os.path.join(TEST_DIR, 'test_session.py')}"
            ]
        else:
            pytest_args = [
                get_python_exec(), "-m", "pytest", "-v",
                "--timeout=100", "--durations=100",
                f"--ignore={os.path.join(TEST_DIR, 'test_session.py')}"
            ]
        
        if not args.no_ignore:
            print(f"Reading ignored tests from {IGNORED_TESTS_FILE}")
            pytest_args.extend(load_pytest_ignore_args())
            
        pytest_args.extend(test_targets)
        
        if args.extra_args:
            pytest_args.extend(shlex.split(args.extra_args))
        
        sys.stdout.flush()
        print(f"Running command: {pytest_args}")
        print(f"PYTHONPATH: {env.get('PYTHONPATH')}")
        result = subprocess.run(pytest_args, cwd=SPARK_CLONE_DIR, env=env)
        
        print("Tests completed.")
        if result.returncode != 0:
            sys.exit(result.returncode)

def do_update_ignore_list(args):
    """Rebuilds the ignore list by running all tests, tracking failures, and regenerating."""
    ensure_venv()
    ensure_spark_clone()
    
    # Read existing flakes to preserve them
    existing_flakes = set()
    if os.path.exists(IGNORED_TESTS_FILE):
        with open(IGNORED_TESTS_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if "# flake" in line:
                    test_id = line.split("#")[0].strip()
                    existing_flakes.add(test_id)
                    
    with BlockingServerManager() as server:
        print("Step 1: Running the full compliance test suite to determine failing tests...")
        
        env = os.environ.copy()
        env["SPARK_CONNECT_TESTING_REMOTE"] = "sc://localhost:12345"
        env["SPARK_TESTING"] = "1"
        env["SPARK_HOME"] = SPARK_DOWNLOAD_DIR
        env["PYTHONPATH"] = os.path.join(SPARK_CLONE_DIR, "python") + (os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else "")
        env["SPARK_SKIP_CONNECT_COMPAT_TESTS"] = "1"
        
        cmd = [
            get_python_exec(), "-m", "pytest", "-n", "auto", "-q", "--tb=no",
            f"--ignore={os.path.join(TEST_DIR, 'test_session.py')}",
            "--timeout=10", "--durations=100"
        ]
        cmd.extend(TARGET_DIRS)
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_path = os.path.join(script_dir, "full_compliance_run.log")
        failed_tests = set()
        
        print(f"Executing tests... Streaming output to console and logging to {log_path}")
        
        with open(log_path, "w") as log_file:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                text=True, cwd=SPARK_CLONE_DIR, env=env
            )
            
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
                print(line, end="")
                
                if server.process.poll() is not None:
                    process.terminate()
                    raise RuntimeError(f"Server process crashed with exit code {server.process.poll()} during test execution.")
                
                line_stripped = line.strip()
                if line_stripped.startswith("FAILED ") or line_stripped.startswith("ERROR "):
                    test_id = line_stripped.split(" ", 1)[1].split(" - ")[0]
                    failed_tests.add(test_id)
                    
            process.wait()
            
        print(f"Discovered {len(failed_tests)} failing/erroring tests.")
        
        print("Step 2: Updating the ignore list to match the result...")
        
        header = []
        if os.path.exists(IGNORED_TESTS_FILE):
            with open(IGNORED_TESTS_FILE, "r") as f:
                for line in f:
                    if line.startswith("#"):
                        header.append(line)
                    else:
                        break
        
        if not header:
            header = [
                "# Tests to ignore in the Spark Connect compliance test suite\n",
                "# Format: <path_to_test>::<ClassName>::<test_method>\n"
            ]
            
        # Combine failed_tests and existing_flakes
        all_tests_to_write = set(failed_tests).union(existing_flakes)
            
        with open(IGNORED_TESTS_FILE, "w") as f:
            f.writelines(header)
            for test in sorted(list(all_tests_to_write)):
                if test in existing_flakes:
                    f.write(f"{test} # flake\n")
                else:
                    f.write(f"{test}\n")
                
        print("Ignore list updated.")

def do_update_flakes(args):
    """Runs tests multiple times to detect flakes and update ignore list."""
    ensure_venv()
    ensure_spark_clone()
    
    num_runs = args.num_flake_runs
    print(f"Running flake detection with {num_runs} runs...")
    
    all_failed_sets = []
    
    with BlockingServerManager() as server:
        for i in range(num_runs):
            print(f"\n--- Run {i+1}/{num_runs} ---")
            failed_this_run = set()
            
            env = os.environ.copy()
            env["SPARK_CONNECT_TESTING_REMOTE"] = "sc://localhost:12345"
            env["SPARK_TESTING"] = "1"
            env["SPARK_HOME"] = SPARK_DOWNLOAD_DIR
            env["PYTHONPATH"] = os.path.join(SPARK_CLONE_DIR, "python") + (os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else "")
            env["SPARK_SKIP_CONNECT_COMPAT_TESTS"] = "1"
            
            cmd = [
                get_python_exec(), "-m", "pytest", "-n", "auto", "-q", "--tb=no",
                f"--ignore={os.path.join(TEST_DIR, 'test_session.py')}",
                "--timeout=10", "--durations=100"
            ]
            test_targets = args.test_targets if args.test_targets else TARGET_DIRS
            cmd.extend(test_targets)
            
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                text=True, cwd=SPARK_CLONE_DIR, env=env
            )
            
            for line in process.stdout:
                print(line, end="")
                line_stripped = line.strip()
                if line_stripped.startswith("FAILED ") or line_stripped.startswith("ERROR "):
                    test_id = line_stripped.split(" ", 1)[1].split(" - ")[0]
                    failed_this_run.add(test_id)
                    
            process.wait()
            all_failed_sets.append(failed_this_run)
            print(f"Run {i+1} completed with {len(failed_this_run)} failures.")
            
    all_ever_failed = set().union(*all_failed_sets)
    
    hard_failures = set()
    flaky_tests = set()
    
    for test in all_ever_failed:
        fail_count = sum(1 for s in all_failed_sets if test in s)
        if fail_count == num_runs:
            hard_failures.add(test)
        else:
            flaky_tests.add(test)
            
    print(f"\nFlake detection summary:")
    print(f"Hard failures: {len(hard_failures)}")
    print(f"Flaky tests: {len(flaky_tests)}")
    
    header = []
    if os.path.exists(IGNORED_TESTS_FILE):
        with open(IGNORED_TESTS_FILE, "r") as f:
            for line in f:
                if line.startswith("#"):
                    header.append(line)
                else:
                    break
    if not header:
        header = [
            "# Tests to ignore in the Spark Connect compliance test suite\n",
            "# Format: <path_to_test>::<ClassName>::<test_method>\n"
        ]
        
    all_tests_to_write = hard_failures.union(flaky_tests)
    with open(IGNORED_TESTS_FILE, "w") as f:
        f.writelines(header)
        for test in sorted(list(all_tests_to_write)):
            if test in flaky_tests:
                f.write(f"{test} # flake\n")
            else:
                f.write(f"{test}\n")
            
    print("Ignore list updated with flake info.")

def do_stats(args):
    """Computes test compliance test coverage."""
    if not os.path.exists(IGNORED_TESTS_FILE):
        print(f"Error: {IGNORED_TESTS_FILE} not found.")
        sys.exit(1)

    # Use nested dicts for categories and subcategories
    ignored_counts = defaultdict(lambda: defaultdict(int))
    total_ignored = 0
    with open(IGNORED_TESTS_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if "#" in line:
                line = line.split("#")[0].strip()
            if line:
                total_ignored += 1
                cat, subcat = extract_category(line)
                ignored_counts[cat][subcat] += 1

    print("Collecting total tests (this may take a few seconds)...")
    env = os.environ.copy()
    env["SPARK_TESTING"] = "1"
    env["SPARK_HOME"] = SPARK_DOWNLOAD_DIR
    env["PYTHONPATH"] = os.path.join(SPARK_CLONE_DIR, "python") + (os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else "")
    env["SPARK_SKIP_CONNECT_COMPAT_TESTS"] = "1"
    
    target_dirs = TARGET_DIRS
    
    cmd = [get_python_exec(), "-m", "pytest", "--collect-only", "-q", 
           f"--ignore={os.path.join(target_dirs[0], 'test_session.py')}"]
    cmd.extend(target_dirs)
    
    result = subprocess.run(
        cmd,
        capture_output=True, text=True, cwd=SPARK_CLONE_DIR, env=env
    )
    
    total_counts = defaultdict(lambda: defaultdict(int))
    total_tests = 0
    for line in result.stdout.splitlines():
        line = line.strip()
        if line and not line.startswith("=") and " warning" not in line and " error" not in line and "::" in line:
            total_tests += 1
            cat, subcat = extract_category(line)
            total_counts[cat][subcat] += 1

    if total_tests == 0:
        print("Could not parse collect-only output. Printing stdout for debug:")
        print(result.stdout)
        sys.exit(1)

    supported_tests = total_tests - total_ignored
    overall_compliance = (supported_tests / total_tests) * 100

    print(f"=================================================")
    print(f"        COMPLIANCE STATUS SUMMARY                ")
    print(f"=================================================")
    print(f"Total Connect Tests : {total_tests}")
    print(f"Supported Tests     : {supported_tests}")
    print(f"Ignored Tests       : {total_ignored}")
    print(f"Compliance Rate     : {overall_compliance:.2f}%")
    print(f"=================================================")
    print(f"        COMPLIANCE BY CATEGORY & SUBCATEGORY     ")
    print(f"=================================================")

    print(f"{'Category':<15} | {'Subcategory':<30} | {'Total':<6} | {'Supp':<5} | {'Ign':<4} | Compliance %")
    print("-" * 80)

    rows = []
    for cat in sorted(total_counts.keys()):
        for subcat in sorted(total_counts[cat].keys()):
            t_count = total_counts[cat][subcat]
            i_count = ignored_counts[cat][subcat]
            s_count = t_count - i_count
            perc = (s_count / t_count) * 100 if t_count > 0 else 0
            if s_count < 0:
                s_count, i_count, perc = 0, t_count, 0.0
            rows.append((cat, subcat, t_count, s_count, i_count, perc))

    # Sort rows by category, then subcategory
    rows.sort(key=lambda x: (x[0], x[1]))

    for cat, subcat, t_count, s_count, i_count, perc in rows:
        print(f"{cat:<15} | {subcat:<30} | {t_count:<6} | {s_count:<5} | {i_count:<4} | {perc:>6.2f}%")

def main():
    parser = argparse.ArgumentParser(description="Spark Connect Compliance Testing Harness")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # run command
    parser_run = subparsers.add_parser("run", help="Run compliance tests")
    parser_run.add_argument("--no-ignore", action="store_true", help="Do not ignore failing tests")
    parser_run.add_argument("--profile", action="store_true", help="Enable profiling with cProfile (disables parallel execution)")
    parser_run.add_argument("--extra-args", help="Extra arguments to pass to pytest")
    parser_run.add_argument("test_targets", nargs="*", help="Specific tests or directories to target")
    parser_run.set_defaults(func=do_run)

    # update-ignore-list command
    parser_update = subparsers.add_parser("update-ignore-list", help="Update ignored_tests.txt based on failures")
    parser_update.set_defaults(func=do_update_ignore_list)

    # update-flakes command
    parser_flakes = subparsers.add_parser("update-flakes", help="Update ignore list with flaky test detection")
    parser_flakes.add_argument("--num-flake-runs", type=int, default=3, help="Number of runs to detect flakes")
    parser_flakes.add_argument("test_targets", nargs="*", help="Specific tests or directories to target")
    parser_flakes.set_defaults(func=do_update_flakes)

    # stats command
    parser_stats = subparsers.add_parser("stats", help="Compute compliance stats")
    parser_stats.set_defaults(func=do_stats)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
