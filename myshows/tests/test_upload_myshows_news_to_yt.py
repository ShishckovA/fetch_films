import gzip
import hashlib
import json
import sys
import tempfile
import types
import unittest
from copy import deepcopy
from pathlib import Path


try:
    import yt.wrapper  # noqa: F401
except ModuleNotFoundError:
    yt_package = types.ModuleType("yt")
    yt_wrapper = types.ModuleType("yt.wrapper")

    class TablePath(str):
        def __new__(cls, path, **attributes):
            instance = super().__new__(cls, path)
            instance.attributes = attributes
            return instance

    class YsonFormat:
        pass

    yt_wrapper.TablePath = TablePath
    yt_wrapper.YsonFormat = YsonFormat
    yt_wrapper.YtClient = object
    yt_package.wrapper = yt_wrapper
    sys.modules["yt"] = yt_package
    sys.modules["yt.wrapper"] = yt_wrapper

import upload_myshows_news_to_yt as uploader


def make_row(news_id=0, *, status="ok"):
    row = {}
    for field_name in uploader.FIELDS:
        if field_name in uploader.LIST_FIELDS:
            row[field_name] = []
        elif field_name in uploader.DICT_FIELDS:
            row[field_name] = {}
        elif field_name in uploader.BOOLEAN_FIELDS:
            row[field_name] = False
        elif field_name in uploader.INTEGER_FIELDS:
            row[field_name] = 0
        else:
            row[field_name] = ""
    for field_name in uploader.OPTIONAL_INTEGER_FIELDS:
        row[field_name] = None
    row.update(
        {
            "source_id": news_id,
            "news_id": news_id if status == "ok" else None,
            "status": status,
            "title": f"News {news_id}",
            "url": f"https://myshows.me/news/{news_id}/",
            "source_file": f"{news_id}.html",
            "source_bytes": 100 + news_id,
            "source_sha256": f"{news_id % 16:x}" * 64,
            "comments_loaded_count": 0 if status == "ok" else None,
            "comments_meta_count": 0 if status == "ok" else None,
            "comments_complete": status == "ok",
        }
    )
    if status == "not_found":
        row["error_status_code"] = 404
        row["error_message"] = "Not Found"
    return row


def write_input(directory: Path, rows):
    input_path = directory / uploader.INPUT_FILENAME
    with gzip.open(input_path, "wt", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    digest = hashlib.sha256(input_path.read_bytes()).hexdigest()
    status_counts = {}
    for row in rows:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
    source_ids = [row["source_id"] for row in rows]
    source_min_id = min(source_ids) if source_ids else None
    source_max_id = max(source_ids) if source_ids else None
    contiguous = bool(source_ids) and source_ids == list(
        range(source_min_id, source_max_id + 1)
    )
    manifest = {
        "schema_version": 1,
        "entity": uploader.ENTITY,
        "complete": True,
        "extraction_complete": True,
        "source_parse_complete": True,
        "fields": list(uploader.FIELDS),
        "record_count": len(rows),
        "source_file_count": len(rows),
        "source_bytes": sum(row["source_bytes"] for row in rows),
        "source_min_id": source_min_id,
        "source_max_id": source_max_id,
        "source_ids_contiguous": contiguous,
        "expected_min_id": source_min_id if contiguous else None,
        "expected_max_id": source_max_id if contiguous else None,
        "comments_loaded_count": sum(
            row["comments_loaded_count"] or 0 for row in rows
        ),
        "comments_meta_count": sum(row["comments_meta_count"] or 0 for row in rows),
        "comments_incomplete_page_count": sum(
            row["comments_loaded_count"] is not None
            and not row["comments_complete"]
            for row in rows
        ),
        "comments_missing_count": sum(
            (row["comments_meta_count"] or 0) - (row["comments_loaded_count"] or 0)
            for row in rows
        ),
        "source_status_counts": status_counts,
        "source_issue_details": [],
        "source_dir": "/source/news",
        "source_inventory_sha256": "a" * 64,
        "source_inventory_sha256_algorithm": "sha256(path\\0size\\0mtime_ns)",
        "source_tree_sha256": "b" * 64,
        "source_tree_sha256_algorithm": "sha256(path\\0content_sha256)",
        "extractor_sha256": "c" * 64,
        "jsonl": str(input_path),
        "jsonl_sha256": digest,
        "jsonl_size_bytes": input_path.stat().st_size,
        "created_at": "2026-07-14T12:00:00+00:00",
    }
    (directory / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    return input_path, manifest


class FakeClient:
    def __init__(self):
        self.tables = {}
        self.write_batches = []
        self.move_calls = []
        self.remove_calls = []

    @staticmethod
    def _attribute_path(path):
        path = str(path)
        if "/@" not in path:
            return path, None
        return tuple(path.rsplit("/@", 1))

    def exists(self, path):
        table_path, attribute = self._attribute_path(path)
        if table_path not in self.tables:
            return False
        if attribute is None:
            return True
        return attribute in self._attributes(table_path)

    def _attributes(self, table_path):
        table = self.tables[table_path]
        attributes = dict(table["attributes"])
        schema = attributes.get("schema", [])
        sorted_by = [
            column["name"] for column in schema if "sort_order" in column
        ]
        attributes.update(
            {
                "row_count": len(table["rows"]),
                "chunk_count": int(bool(table["rows"])),
                "data_weight": sum(
                    len(json.dumps(row, ensure_ascii=False)) for row in table["rows"]
                ),
                "sorted": bool(sorted_by),
                "sorted_by": sorted_by,
            }
        )
        return attributes

    def get(self, path):
        table_path, attribute = self._attribute_path(path)
        if attribute is None:
            return self.tables[table_path]
        return deepcopy(self._attributes(table_path)[attribute])

    def create(self, node_type, path, attributes):
        assert node_type == "table"
        assert path not in self.tables
        self.tables[path] = {"attributes": deepcopy(attributes), "rows": []}

    def write_table(self, table_path, rows, format):
        del format
        path = str(table_path)
        batch = list(rows)
        append = getattr(table_path, "attributes", {}).get("append", False)
        if not append:
            self.tables[path]["rows"] = []
        self.tables[path]["rows"].extend(deepcopy(batch))
        self.write_batches.append((path, batch))

    def read_table(self, table_path, format):
        del format
        path = str(table_path)
        attributes = getattr(table_path, "attributes", {})
        start = attributes.get("start_index", 0)
        end = attributes.get("end_index", len(self.tables[path]["rows"]))
        columns = attributes.get("columns")
        rows = deepcopy(self.tables[path]["rows"][start:end])
        if columns is None:
            return rows
        return [{column: row[column] for column in columns} for row in rows]

    def set(self, path, value):
        table_path, attribute = self._attribute_path(path)
        self.tables[table_path]["attributes"][attribute] = deepcopy(value)

    def move(self, source, destination, force=False):
        if destination in self.tables and not force:
            raise RuntimeError("destination exists")
        self.move_calls.append((source, destination, force))
        self.tables[destination] = self.tables.pop(source)

    def remove(self, path, recursive=False, force=False):
        self.remove_calls.append((path, recursive, force))
        self.tables.pop(path, None)


class UploadMyshowsNewsToYtTest(unittest.TestCase):
    def test_schema_has_exact_typed_sorted_contract(self):
        schema = uploader.table_schema()
        self.assertEqual(len(schema), 66)
        self.assertEqual([column["name"] for column in schema], list(uploader.FIELDS))
        self.assertEqual(
            [column["name"] for column in schema if "sort_order" in column],
            ["source_id"],
        )
        types = {column["name"]: column["type"] for column in schema}
        self.assertEqual(types["source_id"], "int64")
        self.assertEqual(types["news_id"], "int64")
        self.assertEqual(types["error_status_code"], "int64")
        self.assertEqual(types["has_spoilers"], "boolean")
        self.assertEqual(types["comments"], "any")
        self.assertEqual(types["nuxt_route"], "string")
        self.assertFalse(
            next(column["required"] for column in schema if column["name"] == "author")
        )

    def test_manifest_is_strict_and_one_to_one(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            _, manifest = write_input(directory, [make_row()])
            self.assertEqual(
                uploader.load_manifest(
                    directory, expected_min_id=0, expected_max_id=0
                ),
                manifest,
            )
            with self.assertRaisesRegex(ValueError, "uploader contract"):
                uploader.load_manifest(
                    directory, expected_min_id=0, expected_max_id=2
                )

            for key, value in (
                ("entity", "news"),
                ("complete", False),
                ("schema_version", 2),
                ("fields", list(reversed(uploader.FIELDS))),
                ("record_count", 2),
                ("source_tree_sha256", "bad"),
                ("jsonl", "other.jsonl.gz"),
            ):
                invalid = dict(manifest)
                invalid[key] = value
                (directory / "manifest.json").write_text(
                    json.dumps(invalid), encoding="utf-8"
                )
                with self.subTest(key=key):
                    with self.assertRaises(ValueError):
                        uploader.load_manifest(
                            directory, expected_min_id=0, expected_max_id=0
                        )

    def test_convert_and_iteration_accept_ids_zero_then_one(self):
        rows = [make_row(0, status="not_found"), make_row(1)]
        converted_error = uploader.convert_row(rows[0], 1)
        self.assertEqual(converted_error["source_id"], 0)
        self.assertNotIn("news_id", converted_error)
        with tempfile.TemporaryDirectory() as directory_name:
            input_path, _ = write_input(Path(directory_name), rows)
            converted = list(
                uploader.iter_rows(
                    input_path, uploader.RowStats(), progress_every=0
                )
            )
        self.assertEqual([row["source_id"] for row in converted], [0, 1])

        invalid = make_row(2)
        invalid["reaction_like_count"] = 1
        with self.assertRaisesRegex(ValueError, "inconsistent reaction_total"):
            uploader.convert_row(invalid, 3)

    def test_iteration_rejects_duplicate_or_decreasing_ids(self):
        with tempfile.TemporaryDirectory() as directory_name:
            input_path, _ = write_input(
                Path(directory_name), [make_row(1), make_row(1)]
            )
            with self.assertRaisesRegex(ValueError, "strictly increasing"):
                list(
                    uploader.iter_rows(
                        input_path, uploader.RowStats(), progress_every=0
                    )
                )

    def test_iteration_rejects_gap_when_manifest_requires_contiguous_ids(self):
        with tempfile.TemporaryDirectory() as directory_name:
            input_path, _ = write_input(
                Path(directory_name), [make_row(0), make_row(2)]
            )
            with self.assertRaisesRegex(ValueError, "not contiguous"):
                list(
                    uploader.iter_rows(
                        input_path,
                        uploader.RowStats(),
                        progress_every=0,
                        expected_first_id=0,
                        expected_last_id=2,
                        require_contiguous_ids=True,
                    )
                )

    def test_upload_is_resumable_atomic_and_idempotent(self):
        rows = [make_row(0, status="not_found"), make_row(1), make_row(2)]
        table_path = "//tmp/news"
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            write_input(directory, rows)
            client = FakeClient()
            client.tables[table_path] = {"attributes": {}, "rows": [{"old": True}]}

            result = uploader.upload(
                client,
                directory,
                table_path,
                force=True,
                progress_every=0,
                batch_rows=2,
                expected_min_id=0,
                expected_max_id=2,
            )

            self.assertEqual(result["status"], "uploaded")
            self.assertEqual(result["row_count"], 3)
            expected_rows = [
                uploader.convert_row(row, index)
                for index, row in enumerate(rows, 1)
            ]
            self.assertEqual(client.tables[table_path]["rows"], expected_rows)
            self.assertEqual([len(batch) for _, batch in client.write_batches], [2, 1])
            self.assertEqual(
                client.move_calls,
                [(table_path + ".__uploading", table_path, True)],
            )
            self.assertEqual(
                client.get(table_path + "/@manifest_source_status_counts"),
                {"not_found": 1, "ok": 2},
            )

            second = uploader.upload(
                client,
                directory,
                table_path,
                force=False,
                progress_every=0,
                batch_rows=2,
                expected_min_id=0,
                expected_max_id=2,
            )
            self.assertEqual(second["status"], "already_present")
            self.assertEqual(len(client.move_calls), 1)

    def test_upload_resumes_only_matching_stage_prefix(self):
        rows = [make_row(0, status="not_found"), make_row(1), make_row(2)]
        table_path = "//tmp/news"
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            input_path, manifest = write_input(directory, rows)
            client = FakeClient()
            client.tables[table_path + ".__uploading"] = {
                "attributes": {
                    "schema": uploader.table_schema(),
                    "source_sha256": uploader.file_sha256(input_path),
                    "schema_sha256": uploader.schema_sha256(),
                    "expected_row_count": manifest["record_count"],
                },
                "rows": [uploader.convert_row(rows[0], 1)],
            }

            result = uploader.upload(
                client,
                directory,
                table_path,
                force=False,
                progress_every=0,
                batch_rows=10,
                expected_min_id=0,
                expected_max_id=2,
            )

            self.assertEqual(result["row_count"], 3)
            expected_rows = [
                uploader.convert_row(row, index)
                for index, row in enumerate(rows, 1)
            ]
            self.assertEqual(client.tables[table_path]["rows"], expected_rows)
            self.assertEqual([len(batch) for _, batch in client.write_batches], [2])

    def test_force_discards_only_incompatible_staging_table(self):
        rows = [make_row(0), make_row(1)]
        table_path = "//tmp/news"
        stage_path = table_path + ".__uploading"
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            write_input(directory, rows)
            client = FakeClient()
            client.tables[stage_path] = {
                "attributes": {
                    "schema": uploader.table_schema(),
                    "source_sha256": "f" * 64,
                    "schema_sha256": uploader.schema_sha256(),
                    "expected_row_count": 2,
                },
                "rows": [],
            }

            result = uploader.upload(
                client,
                directory,
                table_path,
                force=True,
                progress_every=0,
                batch_rows=10,
                expected_min_id=0,
                expected_max_id=1,
            )

            self.assertEqual(result["status"], "uploaded")
            self.assertEqual(
                client.remove_calls, [(stage_path, True, True)]
            )
            self.assertEqual(
                client.tables[table_path]["rows"],
                [
                    uploader.convert_row(row, index)
                    for index, row in enumerate(rows, 1)
                ],
            )


if __name__ == "__main__":
    unittest.main()
