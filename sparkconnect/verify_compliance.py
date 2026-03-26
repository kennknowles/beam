import sys
from types import ModuleType

# Mock _bz2 for pandas
if "_bz2" not in sys.modules:
    mock_bz2 = ModuleType("_bz2")
    mock_bz2.BZ2Compressor = type("BZ2Compressor", (), {})
    mock_bz2.BZ2Decompressor = type("BZ2Decompressor", (), {})
    sys.modules["_bz2"] = mock_bz2

# Mock runtime_version for newest protobuf generated code
try:
    from google.protobuf import runtime_version
except ImportError:
    mock_rv = ModuleType("runtime_version")
    mock_rv.Domain = type("Domain", (), {"PUBLIC": 1})
    mock_rv.ValidateProtobufRuntimeVersion = lambda *args, **kwargs: None
    
    # We need to make sure google.protobuf exists
    if "google.protobuf" not in sys.modules:
        sys.modules["google.protobuf"] = ModuleType("google.protobuf")
    
    sys.modules["google.protobuf.runtime_version"] = mock_rv

import grpc
import pandas
import pyarrow
print(f"DEBUG: grpc version: {grpc.__version__}")
print(f"DEBUG: pandas version: {pandas.__version__}")
print(f"DEBUG: pyarrow version: {pyarrow.__version__}")
print(f"DEBUG: sys.path: {sys.path}")

import unittest
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as SF
from pyspark.sql.types import Row

class VerifyComplianceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        remote = os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "sc://localhost:12345")
        print(f"Connecting to remote: {remote}")
        cls.spark = SparkSession.builder.remote(remote).getOrCreate()
        cls.testData = [Row(id=i, name=str(i)) for i in range(100)]
        cls.df = cls.spark.createDataFrame(cls.testData)

    def test_endswith(self):
        # Verify endsWith functional compliance
        print("Testing endsWith...")
        results = self.df.filter(self.df.name.endswith("0")).collect()
        self.assertEqual(len(results), 10)
        for row in results:
            self.assertTrue(row.name.endswith("0"))
        print("endsWith test passed!")

    def test_limit(self):
        # Verify LIMIT support in toRowList/collect
        print("Testing limit...")
        results = self.df.limit(5).collect()
        self.assertEqual(len(results), 5)
        print("limit test passed!")

if __name__ == "__main__":
    unittest.main()
