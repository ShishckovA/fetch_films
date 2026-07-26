import csv
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from extract_shikimori import COLUMNS
from upload_shikimori_to_yt import column_type, iter_rows, table_schema


class UploadShikimoriToYtTest(unittest.TestCase):
    def test_schema_preserves_column_order_and_types(self):
        schema = table_schema()

        self.assertEqual([column["name"] for column in schema], COLUMNS)
        self.assertEqual(column_type("title"), "string")
        self.assertEqual(column_type("canonical_id"), "int64")
        self.assertEqual(column_type("comments_count"), "int64")
        self.assertEqual(column_type("score"), "double")
        self.assertEqual(column_type("is_adult"), "boolean")

    def test_rows_are_typed_and_empty_values_are_omitted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.tsv"
            source = {column: "" for column in COLUMNS}
            source.update(
                {
                    "source_file": "1.html",
                    "page_status": "ok",
                    "canonical_id": "1",
                    "comments_count": "0",
                    "score": "8.25",
                    "is_adult": "0",
                }
            )
            with path.open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=COLUMNS, delimiter="\t")
                writer.writeheader()
                writer.writerow(source)

            statuses = Counter()
            rows = list(iter_rows(path, statuses, progress_every=0))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["canonical_id"], 1)
        self.assertEqual(rows[0]["comments_count"], 0)
        self.assertEqual(rows[0]["score"], 8.25)
        self.assertIs(rows[0]["is_adult"], False)
        self.assertNotIn("title", rows[0])
        self.assertEqual(statuses, {"ok": 1})


if __name__ == "__main__":
    unittest.main()
