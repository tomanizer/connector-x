from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("odbc_route_bench.py")
SPEC = importlib.util.spec_from_file_location("odbc_route_bench", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class OdbcRouteBenchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_environ = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self.original_environ)

    def test_load_workloads_prefers_backend_specific_query(self) -> None:
        os.environ["CX_ODBC_COMPARE_MIXED_QUERY"] = "select * from common_mixed"
        os.environ["DB2_COMPARE_MIXED_QUERY"] = "select * from db2_mixed"
        os.environ["CX_ODBC_COMPARE_WIDE_QUERY"] = "select * from common_wide"
        os.environ["DB2_COMPARE_PARTITION_QUERY"] = "select * from db2_partitioned"
        os.environ["DB2_COMPARE_PARTITION_COLUMN"] = "id"
        os.environ["DB2_COMPARE_PARTITION_RANGE"] = "1,1000"
        os.environ["DB2_COMPARE_PARTITION_NUM"] = "4"
        os.environ["DB2_COMPARE_PARTITION_SORT_COLUMNS"] = "id"

        workloads = MODULE.load_workloads(MODULE.BACKENDS["db2"])

        self.assertEqual(
            workloads,
            [
                MODULE.Workload(name="mixed", query="select * from db2_mixed"),
                MODULE.Workload(name="wide", query="select * from common_wide"),
                MODULE.Workload(
                    name="partition",
                    query="select * from db2_partitioned",
                    sort_columns=("id",),
                    partition_on="id",
                    partition_range=(1, 1000),
                    partition_num=4,
                ),
            ],
        )

    def test_load_workloads_rejects_incomplete_partition_config(self) -> None:
        os.environ["SYBASE_COMPARE_PARTITION_QUERY"] = "select * from dbo.large_table"
        os.environ["SYBASE_COMPARE_PARTITION_COLUMN"] = "id"

        with self.assertRaisesRegex(ValueError, "missing range, num"):
            MODULE.load_workloads(MODULE.BACKENDS["sybase"])

    def test_compare_summaries_detects_schema_and_hash_mismatches(self) -> None:
        reference = {
            "row_count": 10,
            "column_names": ["id", "amount"],
            "polars_schema": {"id": "Int64", "amount": "Decimal(10, 2)"},
            "arrow_schema": [
                "pyarrow.Field<id: int64>",
                "pyarrow.Field<amount: decimal128(10, 2)>",
            ],
            "null_counts": {"id": 0, "amount": 1},
            "row_hash_sum": 123,
            "sample_rows": [{"id": 1, "amount": "10.00"}],
            "minmax": {"id": {"min": 1, "max": 10}},
            "decimal_fields": {"amount": {"precision": 10, "scale": 2}},
        }
        candidate = {
            **reference,
            "row_hash_sum": 999,
            "decimal_fields": {"amount": {"precision": 12, "scale": 4}},
        }

        self.assertEqual(
            MODULE.compare_summaries(reference, candidate),
            ["row_hash_sum", "decimal_fields"],
        )


if __name__ == "__main__":
    unittest.main()
