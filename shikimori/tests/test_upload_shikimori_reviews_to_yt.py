import gzip
import hashlib
import json
import sys
import tempfile
import types
import unittest
from copy import deepcopy
from pathlib import Path
from unittest import mock


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

import upload_shikimori_reviews_to_yt as uploader


def make_row(
    *,
    anime_id=1,
    usefulness_rank=1,
    review_id=10,
    votes_for=3,
    votes_against=1,
):
    votes_total = votes_for + votes_against
    row = {
        "anime_id": anime_id,
        "usefulness_rank": usefulness_rank,
        "review_id": review_id,
        "topic_id": review_id + 1000,
        "anime_slug": f"{anime_id}-anime",
        "anime_title": "Anime",
        "anime_url": f"https://example.test/animes/{anime_id}-anime",
        "review_url": f"https://example.test/reviews/{review_id}",
        "author_id": review_id + 2000,
        "author_nickname": "Author",
        "author_nickname_snapshot": "Author",
        "author_url": "https://example.test/Author",
        "author_avatar_url": "https://example.test/avatar.png",
        "author_avatar_srcset": "https://example.test/avatar@2x.png 2x",
        "opinion": "positive",
        "user_score": 8,
        "user_list_status": "completed",
        "votes_for": votes_for,
        "votes_against": votes_against,
        "votes_total": votes_total,
        "usefulness_score": votes_for - votes_against,
        "comments_count": 2,
        "created_at": "2026-01-01T01:02:03+03:00",
        "published_at": "2026-01-01T01:02:03+03:00",
        "updated_at": "2026-01-02T01:02:03+03:00",
        "is_written_before_release": False,
        "body_text": "Review text",
        "body_html": "<p>Review text</p>",
        "body_links": [{"href": "https://example.test", "text": "link"}],
        "body_images": [{"src": "https://example.test/image.png"}],
        "inline_spoilers_count": 0,
        "block_spoilers_count": 0,
        "source_file": f"{anime_id}-anime/1.json",
        "source_page": 1,
        "source_position": usefulness_rank,
    }
    if votes_total:
        row["usefulness_ratio"] = votes_for / votes_total
    return row


def write_input(directory: Path, rows, *, source_pagination_complete=False):
    input_path = directory / "reviews.jsonl.gz"
    with gzip.open(input_path, "wt", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    digest = hashlib.sha256(input_path.read_bytes()).hexdigest()
    manifest = {
        "entity": "reviews",
        "complete": True,
        "extraction_complete": True,
        "schema_version": 1,
        "fields": list(uploader.FIELDS),
        "record_count": len(rows),
        "source_tree_sha256": "a" * 64,
        "jsonl_sha256": digest,
        "ranking_method": uploader.RANKING_METHOD,
        "source_file_count": 3,
        "source_pagination_complete": source_pagination_complete,
        "source_status_counts": {"ok": 2, "transient_unavailable": 1},
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
        sort_columns = [
            column["name"] for column in schema if "sort_order" in column
        ]
        attributes.update(
            {
                "row_count": len(table["rows"]),
                "chunk_count": int(bool(table["rows"])),
                "data_weight": sum(
                    len(json.dumps(row, ensure_ascii=False)) for row in table["rows"]
                ),
                "sorted": bool(sort_columns),
                "sorted_by": sort_columns,
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


class UploadShikimoriReviewsToYtTest(unittest.TestCase):
    def test_schema_has_typed_sorted_prefix(self):
        schema = uploader.table_schema()

        self.assertEqual([column["name"] for column in schema], list(uploader.FIELDS))
        self.assertEqual(len(schema), 36)
        self.assertEqual(
            [column["name"] for column in schema if "sort_order" in column],
            ["anime_id", "usefulness_rank", "review_id"],
        )
        self.assertEqual(
            {column["name"] for column in schema if column["required"]},
            set(uploader.FIELDS) - uploader.OPTIONAL_FIELDS - uploader.ANY_FIELDS,
        )
        types = {column["name"]: column["type"] for column in schema}
        self.assertEqual(types["review_id"], "int64")
        self.assertEqual(types["usefulness_ratio"], "double")
        self.assertEqual(types["is_written_before_release"], "boolean")
        self.assertEqual(types["body_links"], "any")
        self.assertEqual(types["body_images"], "any")
        self.assertFalse(next(
            column["required"] for column in schema if column["name"] == "body_links"
        ))
        self.assertNotIn("author_score", types)

    def test_manifest_requires_complete_count_and_source_tree_digest(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            valid = {
                "entity": "reviews",
                "complete": True,
                "extraction_complete": True,
                "schema_version": 1,
                "fields": list(uploader.FIELDS),
                "record_count": 0,
                "source_tree_sha256": "b" * 64,
                "jsonl_sha256": "c" * 64,
                "source_pagination_complete": True,
                "ranking_method": uploader.RANKING_METHOD,
                "extra_key": "accepted",
            }
            path = directory / "manifest.json"
            path.write_text(json.dumps(valid), encoding="utf-8")
            self.assertEqual(uploader.load_manifest(directory), valid)

            for key, value in (
                ("complete", False),
                ("entity", "animes"),
                ("extraction_complete", False),
                ("schema_version", 2),
                ("fields", list(reversed(uploader.FIELDS))),
                ("record_count", True),
                ("record_count", -1),
                ("source_tree_sha256", "not-a-sha256"),
                ("jsonl_sha256", "not-a-sha256"),
                ("source_pagination_complete", "false"),
                ("ranking_method", "review_id DESC"),
            ):
                invalid = dict(valid)
                invalid[key] = value
                path.write_text(json.dumps(invalid), encoding="utf-8")
                with self.assertRaises(ValueError):
                    uploader.load_manifest(directory)

            for missing_key in (
                "entity",
                "complete",
                "extraction_complete",
                "schema_version",
                "fields",
                "record_count",
                "source_tree_sha256",
                "jsonl_sha256",
                "source_pagination_complete",
                "ranking_method",
            ):
                invalid = dict(valid)
                invalid.pop(missing_key)
                path.write_text(json.dumps(invalid), encoding="utf-8")
                with self.subTest(missing_key=missing_key):
                    with self.assertRaises(ValueError):
                        uploader.load_manifest(directory)

    def test_convert_row_validates_types_and_derived_vote_fields(self):
        row = make_row(votes_for=0, votes_against=0)
        row.pop("user_score")
        row.pop("user_list_status")

        converted = uploader.convert_row(row, 1)

        self.assertNotIn("usefulness_ratio", converted)
        self.assertNotIn("user_score", converted)
        self.assertEqual(converted["body_images"][0]["src"], "https://example.test/image.png")

        invalid = make_row()
        invalid["votes_total"] = 100
        with self.assertRaisesRegex(ValueError, "inconsistent votes_total"):
            uploader.convert_row(invalid, 1)

    def test_iter_rows_requires_sorted_keys_contiguous_ranks_and_unique_reviews(self):
        valid_rows = [
            make_row(anime_id=1, usefulness_rank=1, review_id=12),
            make_row(anime_id=1, usefulness_rank=2, review_id=11),
            make_row(anime_id=2, usefulness_rank=1, review_id=20),
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            input_path, _ = write_input(Path(directory_name), valid_rows)
            stats = uploader.RowStats()
            rows = list(uploader.iter_rows(input_path, stats, progress_every=0))
            self.assertEqual(len(rows), 3)
            self.assertEqual(stats.row_count, 3)

            invalid_rows = [valid_rows[0], make_row(usefulness_rank=3, review_id=13)]
            input_path, _ = write_input(Path(directory_name), invalid_rows)
            with self.assertRaisesRegex(ValueError, "expected usefulness_rank 2"):
                list(
                    uploader.iter_rows(
                        input_path, uploader.RowStats(), progress_every=0
                    )
                )

            wrong_order = [
                valid_rows[0],
                make_row(
                    anime_id=1,
                    usefulness_rank=2,
                    review_id=11,
                    votes_for=5,
                    votes_against=1,
                ),
            ]
            input_path, _ = write_input(Path(directory_name), wrong_order)
            with self.assertRaisesRegex(ValueError, "rows do not follow"):
                list(
                    uploader.iter_rows(
                        input_path, uploader.RowStats(), progress_every=0
                    )
                )

    def test_upload_rejects_incomplete_source_by_default(self):
        table_path = "//tmp/reviews"
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            write_input(directory, [make_row()])

            with self.assertRaisesRegex(ValueError, "Source pagination is incomplete"):
                uploader.upload(
                    FakeClient(),
                    directory,
                    table_path,
                    force=False,
                    progress_every=0,
                    batch_rows=10,
                )

    def test_upload_uses_resumable_stage_and_atomic_force_move(self):
        rows = [
            make_row(anime_id=1, usefulness_rank=1, review_id=12),
            make_row(anime_id=1, usefulness_rank=2, review_id=11),
            make_row(anime_id=2, usefulness_rank=1, review_id=20),
        ]
        table_path = "//tmp/reviews"
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
                allow_incomplete_source=True,
            )

            self.assertEqual(result["status"], "uploaded")
            self.assertEqual(client.tables[table_path]["rows"], rows)
            self.assertEqual([len(batch) for _, batch in client.write_batches], [2, 1])
            self.assertEqual(
                client.move_calls,
                [(table_path + ".__uploading", table_path, True)],
            )
            self.assertFalse(client.exists(table_path + ".__uploading"))
            self.assertFalse(
                client.get(table_path + "/@manifest_source_pagination_complete")
            )

            second = uploader.upload(
                client,
                directory,
                table_path,
                force=False,
                progress_every=0,
                batch_rows=2,
                allow_incomplete_source=True,
            )
            self.assertEqual(second["status"], "already_present")
            self.assertEqual(len(client.move_calls), 1)

    def test_upload_resumes_compatible_stage_by_row_count(self):
        rows = [
            make_row(anime_id=1, usefulness_rank=1, review_id=12),
            make_row(anime_id=1, usefulness_rank=2, review_id=11),
            make_row(anime_id=2, usefulness_rank=1, review_id=20),
        ]
        table_path = "//tmp/reviews"
        stage_path = table_path + ".__uploading"
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            input_path, manifest = write_input(directory, rows)
            client = FakeClient()
            client.create(
                "table",
                stage_path,
                attributes={
                    "schema": uploader.table_schema(),
                    "source_sha256": uploader.file_sha256(input_path),
                    "source_tree_sha256": manifest["source_tree_sha256"],
                    "schema_sha256": uploader.schema_sha256(),
                    "expected_row_count": len(rows),
                },
            )
            client.tables[stage_path]["rows"].append(deepcopy(rows[0]))

            result = uploader.upload(
                client,
                directory,
                table_path,
                force=False,
                progress_every=0,
                batch_rows=10,
                allow_incomplete_source=True,
            )

            self.assertEqual(result["status"], "uploaded")
            self.assertEqual(client.tables[table_path]["rows"], rows)
            self.assertEqual(len(client.write_batches), 1)
            self.assertEqual(client.write_batches[0][1], rows[1:])

    def test_resume_rejects_remote_checkpoint_key_mismatch(self):
        rows = [make_row(anime_id=1, usefulness_rank=1, review_id=12)]
        table_path = "//tmp/reviews"
        stage_path = table_path + ".__uploading"
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            input_path, manifest = write_input(directory, rows)
            client = FakeClient()
            client.create(
                "table",
                stage_path,
                attributes={
                    "schema": uploader.table_schema(),
                    "source_sha256": uploader.file_sha256(input_path),
                    "source_tree_sha256": manifest["source_tree_sha256"],
                    "schema_sha256": uploader.schema_sha256(),
                    "expected_row_count": 1,
                },
            )
            wrong_checkpoint = deepcopy(rows[0])
            wrong_checkpoint["review_id"] = 999
            client.tables[stage_path]["rows"].append(wrong_checkpoint)

            with self.assertRaisesRegex(RuntimeError, "checkpoint key differs"):
                uploader.upload(
                    client,
                    directory,
                    table_path,
                    force=False,
                    progress_every=0,
                    batch_rows=10,
                    allow_incomplete_source=True,
                )

    def test_main_configures_parallel_ordered_writes(self):
        client = mock.Mock()
        client.exists.return_value = True
        with mock.patch.object(uploader.yt, "YtClient", return_value=client) as factory:
            with mock.patch.object(
                uploader,
                "upload",
                return_value={"status": "already_present"},
            ) as upload_mock:
                self.assertEqual(
                    uploader.main(
                        [
                            "--proxy",
                            "cluster",
                            "--table",
                            "//tmp/reviews",
                            "--allow-incomplete-source",
                        ]
                    ),
                    0,
                )

        config = factory.call_args.kwargs["config"]
        self.assertTrue(config["write_parallel"]["enable"])
        self.assertEqual(config["write_parallel"]["max_thread_count"], 4)
        self.assertFalse(config["write_parallel"]["unordered"])
        self.assertTrue(upload_mock.call_args.kwargs["allow_incomplete_source"])


if __name__ == "__main__":
    unittest.main()
