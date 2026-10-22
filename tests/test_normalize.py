"""Kiểm tra Spark bằng dữ liệu mẫu trong thư mục tạm local, không ghi lên HDFS."""

import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
import normalize as job


class NormalizationChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]").appName("CheckChicagoNormalization")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .getOrCreate()
        )
        job.configure_spark(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="chicago-normalize-check-")
        self.root = Path(self.workspace.name)
        self.bronze = self.root / "bronze"
        self.silver = self.root / "silver"
        self.bronze.mkdir()

    def tearDown(self):
        self.workspace.cleanup()

    def write_csv(self, path, rows, typo=False):
        path.parent.mkdir(parents=True, exist_ok=True)
        header = list(job.SOURCE_COLUMNS)
        if typo:
            header[3] = "primery_description"
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)

    def run_job(self):
        return job.normalize_data(
            self.spark, self.bronze.as_uri(), self.silver.as_uri(),
            require_three_datanodes=False,
        )

    def read_silver(self):
        return self.spark.read.schema(job.SILVER_SCHEMA).parquet(self.silver.as_uri())

    def test_parquet_types_partitions_dedup_rerun_and_invalid_input(self):
        original = [
            "1/2/2018 3:04", " 001XX  W TEST ST ", " theft ", " simple ",
            " street ", "TRUE", "0", "25.0", "1999", "41.9", "-87.6", "ignored",
        ]
        bad_date = list(original)
        bad_date[0] = "not-a-date"
        missing_type = list(original)
        missing_type[2] = "   "
        outside = list(original)
        outside[0] = "1/1/2017 0:00"
        quoted = [
            "2/3/2020 11:59:58 PM", "002XX W TEST ST", "battery",
            'He said "hello", then left', "", "1", "FALSE", "3",
            "2020", "42.0", "-87.7", "",
        ]
        first = self.bronze / "ingest_date=2026-10-15" / "a.csv"
        self.write_csv(first, [
            original, bad_date, missing_type, outside, quoted,
            original + ["extra-field"], original[:-1],
        ])
        with first.open("a", encoding="utf-8") as handle:
            handle.write('"unclosed quote\n\n')
        duplicate = [
            "2018-01-02T03:04:00", "001XX W TEST ST", "THEFT", "SIMPLE",
            "STREET", "1", "false", "25", "2018", "41.900000000", "-87.600", "",
        ]
        optional_invalid = [
            "12/31/2023 23:45", "003XX W TEST ST", "BATTERY", "simple",
            "", "maybe", "FALSE", "78", "2023", "", "", "",
        ]
        utc_date = [
            "2019-07-01T02:00:00Z", "004XX W TEST ST", "BURGLARY", "other",
            "OTHER", "FALSE", "TRUE", "abc", "2019", "NaN", "Infinity", "ignored",
        ]
        self.write_csv(
            self.bronze / "ingest_date=2026-10-16" / "b.csv",
            [duplicate, optional_invalid, utc_date], typo=True,
        )
        (self.bronze / "_ignore.csv").write_text("wrong header", encoding="utf-8")

        result = self.run_job()
        self.assertEqual(
            {key: result[key] for key in (
                "input_files", "input_rows", "rejected_rows", "duplicates_removed", "output_rows"
            )},
            {"input_files": 2, "input_rows": 11, "rejected_rows": 6,
             "duplicates_removed": 1, "output_rows": 4},
        )
        self.assertEqual(result["errors"], {
            "csv_invalid": 3, "date_invalid": 1,
            "primary_type_missing": 1, "year_out_of_range": 1,
        })
        data = self.read_silver()
        rows = {row.city_block: row for row in data.collect()}
        self.assertEqual(len(rows), 4)
        first_row = rows["001XX W TEST ST"]
        self.assertEqual((first_row.year, first_row.month), (2018, "01"))
        self.assertIs(first_row.arrest, True)
        self.assertIs(first_row.domestic, False)
        self.assertEqual(first_row.community_area, 25)
        self.assertEqual(first_row.location, "(41.9, -87.6)")
        self.assertEqual(rows["002XX W TEST ST"].primary_description, 'HE SAID "HELLO", THEN LEFT')
        self.assertEqual(rows["002XX W TEST ST"].month, "02")
        missing = rows["003XX W TEST ST"]
        self.assertIsNone(missing.latitude)
        self.assertIsNone(missing.location)
        self.assertIsNone(missing.arrest)
        self.assertIsNone(missing.community_area)
        self.assertIsNone(missing.location_description)
        self.assertEqual(rows["004XX W TEST ST"].month, "06")  # UTC -> America/Chicago.
        self.assertIsNone(rows["004XX W TEST ST"].latitude)
        self.assertIsNone(rows["004XX W TEST ST"].longitude)
        self.assertEqual(len(list(self.silver.glob("year=*/month=*"))), 72)
        self.assertTrue((self.silver / "year=2018" / "month=01").is_dir())
        self.assertFalse((self.silver / "year=2018" / "month=1").exists())

        parquet_file = next(self.silver.rglob("*.parquet"))
        jvm = self.spark._jvm
        footer = jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(
            self.spark.sparkContext._jsc.hadoopConfiguration(),
            jvm.org.apache.hadoop.fs.Path(str(parquet_file)),
        )
        codec = footer.getBlocks().get(0).getColumns().get(0).getCodec().toString()
        self.assertEqual(codec, "SNAPPY")
        before = set(tuple(row) for row in data.collect())
        self.assertEqual(self.run_job(), result)
        self.assertEqual(set(tuple(row) for row in self.read_silver().collect()), before)

        with mock.patch(
            "pyspark.sql.readwriter.DataFrameWriter.parquet",
            side_effect=RuntimeError("lỗi ghi mô phỏng"),
        ):
            with self.assertRaisesRegex(RuntimeError, "lỗi ghi mô phỏng"):
                self.run_job()
        self.assertEqual(set(tuple(row) for row in self.read_silver().collect()), before)

        invalid_bronze = self.root / "invalid-bronze"
        self.write_csv(invalid_bronze / "invalid.csv", [bad_date])
        with self.assertRaisesRegex(RuntimeError, "Không có dòng hợp lệ"):
            job.normalize_data(
                self.spark, invalid_bronze.as_uri(), self.silver.as_uri(),
                require_three_datanodes=False,
            )
        self.assertEqual(set(tuple(row) for row in self.read_silver().collect()), before)
        self.assertEqual(list((self.root / "_staging").iterdir()), [])

    def test_rollback_after_failed_publish(self):
        self.silver.mkdir()
        (self.silver / "old-data.txt").write_text("original", encoding="utf-8")
        stage = self.root / "stage"
        stage.mkdir()
        (stage / "new-data.txt").write_text("new", encoding="utf-8")
        backup = self.root / "backup"
        fs, stage_path = job._filesystem(self.spark, stage.as_uri())
        _, silver_path = job._filesystem(self.spark, self.silver.as_uri())
        _, backup_path = job._filesystem(self.spark, backup.as_uri())

        class FailPromotion:
            def __getattr__(self, name):
                return getattr(fs, name)

            def rename(self, source, target):
                if source.toString() == stage_path.toString():
                    return False
                return fs.rename(source, target)

        with self.assertRaisesRegex(RuntimeError, "đã giữ/khôi phục"):
            job._publish_silver(FailPromotion(), stage_path, silver_path, backup_path)
        self.assertEqual((self.silver / "old-data.txt").read_text(), "original")
        self.assertFalse(backup.exists())
        self.assertTrue((stage / "new-data.txt").is_file())

    def test_empty_bronze_and_wrong_header_preserve_silver(self):
        self.silver.mkdir()
        marker = self.silver / "marker"
        marker.write_text("keep", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "không có file CSV"):
            self.run_job()
        (self.bronze / "wrong.csv").write_text("date_reported,primary_type\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "Header không đúng"):
            self.run_job()
        self.assertEqual(marker.read_text(), "keep")


if __name__ == "__main__":
    unittest.main(verbosity=2)
