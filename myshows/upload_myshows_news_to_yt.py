#!/usr/bin/env python3
"""Upload parsed MyShows news pages to a typed, sorted static YT table."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import yt.wrapper as yt


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = PROJECT_DIR / "news_parsed"
DEFAULT_TABLE = (
    "//home/hc/ml-research/tmp-alexey.shishkov/film_data/myshow/news"
)
INPUT_FILENAME = "news.jsonl.gz"
ENTITY = "myshows_news"
DEFAULT_EXPECTED_MIN_ID = 0
DEFAULT_EXPECTED_MAX_ID = 12558

# Keep this tuple byte-for-byte aligned with extract_myshows_news.py. The manifest
# also carries it, so a parser/uploader schema mismatch fails before any YT write.
FIELDS = (
    "source_id",
    "news_id",
    "status",
    "error_status_code",
    "error_message",
    "slug",
    "url",
    "canonical_url",
    "page_title",
    "title",
    "foreword",
    "content_text",
    "content_html",
    "published_at",
    "modified_at",
    "source_site",
    "image_url",
    "image_width",
    "image_height",
    "image_alt",
    "media_source",
    "video_html",
    "author_id",
    "author_name",
    "author_url",
    "author_description",
    "author_articles_count",
    "category_title",
    "category_slug",
    "comments_total",
    "comments_new",
    "comments_loaded_count",
    "comments_meta_count",
    "comments_complete",
    "has_spoilers",
    "reaction_like_count",
    "reaction_fire_count",
    "reaction_dislike_count",
    "reaction_love_count",
    "reaction_anger_count",
    "reaction_shock_count",
    "reaction_total",
    "images",
    "tags",
    "content_links",
    "content_images",
    "similar_news",
    "categories",
    "comments",
    "comments_meta",
    "comments_with_images",
    "read_also_aside",
    "read_also_main",
    "emotions",
    "author",
    "catalog_links",
    "seo_meta",
    "hreflang_links",
    "json_ld",
    "nuxt_news",
    "nuxt_route",
    "nuxt_errors",
    "parse_warnings",
    "source_file",
    "source_bytes",
    "source_sha256",
)

SORT_FIELDS = ("source_id",)
STATUS_VALUES = {
    "ok",
    "not_found",
    "nuxt_error",
    "missing_news",
    "parse_error",
}
OPTIONAL_INTEGER_FIELDS = {
    "news_id",
    "error_status_code",
    "image_width",
    "image_height",
    "author_id",
    "author_articles_count",
    "comments_total",
    "comments_new",
    "comments_loaded_count",
    "comments_meta_count",
}
INTEGER_FIELDS = OPTIONAL_INTEGER_FIELDS | {
    "source_id",
    "reaction_like_count",
    "reaction_fire_count",
    "reaction_dislike_count",
    "reaction_love_count",
    "reaction_anger_count",
    "reaction_shock_count",
    "reaction_total",
    "source_bytes",
}
BOOLEAN_FIELDS = {"comments_complete", "has_spoilers"}
LIST_FIELDS = {
    "images",
    "tags",
    "content_links",
    "content_images",
    "similar_news",
    "categories",
    "comments",
    "comments_with_images",
    "read_also_aside",
    "read_also_main",
    "emotions",
    "catalog_links",
    "seo_meta",
    "hreflang_links",
    "json_ld",
    "parse_warnings",
}
DICT_FIELDS = {"comments_meta", "author", "nuxt_news", "nuxt_errors"}
ANY_FIELDS = LIST_FIELDS | DICT_FIELDS
OPTIONAL_FIELDS = OPTIONAL_INTEGER_FIELDS
REACTION_FIELDS = (
    "reaction_like_count",
    "reaction_fire_count",
    "reaction_dislike_count",
    "reaction_love_count",
    "reaction_anger_count",
    "reaction_shock_count",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

MANIFEST_REQUIRED_FIELDS = {
    "schema_version",
    "entity",
    "complete",
    "extraction_complete",
    "source_parse_complete",
    "fields",
    "record_count",
    "source_file_count",
    "source_bytes",
    "source_min_id",
    "source_max_id",
    "source_ids_contiguous",
    "expected_min_id",
    "expected_max_id",
    "comments_loaded_count",
    "comments_meta_count",
    "comments_incomplete_page_count",
    "comments_missing_count",
    "source_status_counts",
    "source_issue_details",
    "source_dir",
    "source_inventory_sha256",
    "source_inventory_sha256_algorithm",
    "source_tree_sha256",
    "source_tree_sha256_algorithm",
    "extractor_sha256",
    "jsonl",
    "jsonl_sha256",
    "jsonl_size_bytes",
    "created_at",
}


@dataclass
class RowStats:
    row_count: int = 0
    first_source_id: int | None = None
    previous_source_id: int | None = None


def field_type(field_name: str) -> str:
    if field_name in INTEGER_FIELDS:
        return "int64"
    if field_name in BOOLEAN_FIELDS:
        return "boolean"
    if field_name in ANY_FIELDS:
        return "any"
    return "string"


def table_schema() -> list[dict[str, Any]]:
    schema = []
    for field_name in FIELDS:
        column: dict[str, Any] = {
            "name": field_name,
            "type": field_type(field_name),
            # Legacy YT schemas do not allow required columns of type any.
            "required": field_name not in OPTIONAL_FIELDS | ANY_FIELDS,
        }
        if field_name in SORT_FIELDS:
            column["sort_order"] = "ascending"
        schema.append(column)
    return schema


def _manifest_nonnegative_int(manifest: dict[str, Any], key: str) -> int:
    value = manifest.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"Invalid manifest {key}: {value!r}")
    return value


def _manifest_sha256(manifest: dict[str, Any], key: str) -> str:
    value = manifest.get(key)
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ValueError(f"Invalid manifest {key}: {value!r}")
    return value


def load_manifest(
    input_dir: Path,
    *,
    expected_min_id: int | None = DEFAULT_EXPECTED_MIN_ID,
    expected_max_id: int | None = DEFAULT_EXPECTED_MAX_ID,
) -> dict[str, Any]:
    contract_min_id = expected_min_id
    contract_max_id = expected_max_id
    if (contract_min_id is None) != (contract_max_id is None):
        raise ValueError("Uploader expected source ID bounds must be set together")
    for name, value in (
        ("expected_min_id", contract_min_id),
        ("expected_max_id", contract_max_id),
    ):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 0
        ):
            raise ValueError(f"Invalid uploader {name}: {value!r}")
    if (
        contract_min_id is not None
        and contract_max_id is not None
        and contract_max_id < contract_min_id
    ):
        raise ValueError("Uploader expected source ID bounds are reversed")

    path = input_dir / "manifest.json"
    with path.open(encoding="utf-8") as file:
        manifest = json.load(file)
    if not isinstance(manifest, dict):
        raise ValueError(f"Invalid manifest: {path}")
    missing = MANIFEST_REQUIRED_FIELDS - set(manifest)
    if missing:
        raise ValueError(f"Manifest is missing fields: {sorted(missing)}")
    if manifest["entity"] != ENTITY:
        raise ValueError(f"Mismatched manifest entity: {manifest['entity']!r}")
    if manifest["complete"] is not True or manifest["extraction_complete"] is not True:
        raise ValueError(f"Incomplete extraction manifest: {path}")
    if not isinstance(manifest["source_parse_complete"], bool):
        raise ValueError("Manifest source_parse_complete must be boolean")
    if manifest["schema_version"] != 1 or isinstance(
        manifest["schema_version"], bool
    ):
        raise ValueError(
            f"Unsupported manifest schema_version: {manifest['schema_version']!r}"
        )
    if manifest["fields"] != list(FIELDS):
        raise ValueError("Manifest fields do not match the uploader schema")

    record_count = _manifest_nonnegative_int(manifest, "record_count")
    source_file_count = _manifest_nonnegative_int(manifest, "source_file_count")
    _manifest_nonnegative_int(manifest, "source_bytes")
    _manifest_nonnegative_int(manifest, "jsonl_size_bytes")
    comments_loaded_count = _manifest_nonnegative_int(
        manifest, "comments_loaded_count"
    )
    comments_meta_count = _manifest_nonnegative_int(manifest, "comments_meta_count")
    comments_incomplete_page_count = _manifest_nonnegative_int(
        manifest, "comments_incomplete_page_count"
    )
    comments_missing_count = _manifest_nonnegative_int(
        manifest, "comments_missing_count"
    )
    if source_file_count != record_count:
        raise ValueError(
            "Expected exactly one row per source file: "
            f"record_count={record_count}, source_file_count={source_file_count}"
        )
    if not isinstance(manifest["source_ids_contiguous"], bool):
        raise ValueError("Manifest source_ids_contiguous must be boolean")
    if record_count:
        source_min_id = _manifest_nonnegative_int(manifest, "source_min_id")
        source_max_id = _manifest_nonnegative_int(manifest, "source_max_id")
        if source_max_id < source_min_id:
            raise ValueError("Manifest source ID bounds are reversed")
        if (
            manifest["source_ids_contiguous"]
            and source_max_id - source_min_id + 1 != record_count
        ):
            raise ValueError("Contiguous source ID bounds do not match record_count")
    elif manifest["source_min_id"] is not None or manifest["source_max_id"] is not None:
        raise ValueError("Empty manifest must have null source ID bounds")
    manifest_expected_min_id = manifest["expected_min_id"]
    manifest_expected_max_id = manifest["expected_max_id"]
    if (manifest_expected_min_id is None) != (manifest_expected_max_id is None):
        raise ValueError("Manifest expected source ID bounds must be set together")
    if manifest_expected_min_id is not None:
        manifest_expected_min_id = _manifest_nonnegative_int(
            manifest, "expected_min_id"
        )
        manifest_expected_max_id = _manifest_nonnegative_int(
            manifest, "expected_max_id"
        )
        if manifest_expected_max_id < manifest_expected_min_id:
            raise ValueError("Manifest expected source ID bounds are reversed")
        if manifest_expected_max_id - manifest_expected_min_id + 1 != record_count:
            raise ValueError("Expected source ID bounds do not match record_count")
        if (
            not manifest["source_ids_contiguous"]
            or manifest["source_min_id"] != manifest_expected_min_id
            or manifest["source_max_id"] != manifest_expected_max_id
        ):
            raise ValueError("Actual source ID coverage differs from expected coverage")
    if (
        manifest_expected_min_id != contract_min_id
        or manifest_expected_max_id != contract_max_id
    ):
        raise ValueError(
            "Manifest expected source range differs from the uploader contract: "
            f"manifest=({manifest_expected_min_id}, {manifest_expected_max_id}), "
            f"uploader=({contract_min_id}, {contract_max_id})"
        )
    if comments_loaded_count > comments_meta_count:
        raise ValueError("Manifest has more loaded comments than metadata comments")
    if comments_missing_count != comments_meta_count - comments_loaded_count:
        raise ValueError("Manifest comments_missing_count is inconsistent")
    if comments_incomplete_page_count > record_count:
        raise ValueError("Manifest comments_incomplete_page_count is too large")

    status_counts = manifest["source_status_counts"]
    if not isinstance(status_counts, dict) or any(
        status not in STATUS_VALUES
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
        for status, count in status_counts.items()
    ):
        raise ValueError("Invalid manifest source_status_counts")
    if sum(status_counts.values()) != record_count:
        raise ValueError("Manifest source_status_counts do not sum to record_count")
    if not isinstance(manifest["source_issue_details"], list):
        raise ValueError("Manifest source_issue_details must be a list")

    for key in (
        "source_dir",
        "source_inventory_sha256_algorithm",
        "source_tree_sha256_algorithm",
        "extractor_sha256",
        "created_at",
    ):
        if not isinstance(manifest[key], str) or not manifest[key]:
            raise ValueError(f"Invalid manifest {key}: {manifest[key]!r}")
    for key in (
        "source_inventory_sha256",
        "source_tree_sha256",
        "extractor_sha256",
        "jsonl_sha256",
    ):
        _manifest_sha256(manifest, key)
    manifest_jsonl = manifest["jsonl"]
    if not isinstance(manifest_jsonl, str) or Path(manifest_jsonl).name != INPUT_FILENAME:
        raise ValueError(
            f"Invalid manifest jsonl basename: expected {INPUT_FILENAME!r}, "
            f"got {manifest_jsonl!r}"
        )
    return manifest


def _convert_integer(field_name: str, value: Any, row_number: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"news row {row_number}: invalid {field_name}")
    return value


def convert_row(source: dict[str, Any], row_number: int) -> dict[str, Any]:
    expected = set(FIELDS)
    actual = set(source)
    if actual != expected:
        raise ValueError(
            f"news row {row_number}: fields mismatch; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )

    result: dict[str, Any] = {}
    for field_name in FIELDS:
        value = source[field_name]
        if value is None:
            if field_name not in OPTIONAL_FIELDS:
                raise ValueError(f"news row {row_number}: null {field_name}")
            continue
        column_type = field_type(field_name)
        if column_type == "int64":
            result[field_name] = _convert_integer(field_name, value, row_number)
        elif column_type == "boolean":
            if not isinstance(value, bool):
                raise ValueError(f"news row {row_number}: invalid {field_name}")
            result[field_name] = value
        elif field_name in LIST_FIELDS:
            if not isinstance(value, list):
                raise ValueError(f"news row {row_number}: invalid {field_name}")
            result[field_name] = value
        elif field_name in DICT_FIELDS:
            if not isinstance(value, dict):
                raise ValueError(f"news row {row_number}: invalid {field_name}")
            result[field_name] = value
        else:
            if not isinstance(value, str):
                raise ValueError(f"news row {row_number}: invalid {field_name}")
            result[field_name] = value

    if result["source_id"] < 0:
        raise ValueError(f"news row {row_number}: negative source_id")
    if result.get("news_id") is not None and result["news_id"] < 0:
        raise ValueError(f"news row {row_number}: negative news_id")
    if result["source_bytes"] <= 0:
        raise ValueError(f"news row {row_number}: non-positive source_bytes")
    if result["status"] not in STATUS_VALUES:
        raise ValueError(f"news row {row_number}: invalid status")
    if result["source_file"] != f"{result['source_id']}.html":
        raise ValueError(f"news row {row_number}: source_file does not match source_id")
    if not SHA256_RE.fullmatch(result["source_sha256"]):
        raise ValueError(f"news row {row_number}: invalid source_sha256")

    for field_name in INTEGER_FIELDS - {"source_id", "news_id", "source_bytes"}:
        if field_name in result and result[field_name] < 0:
            raise ValueError(f"news row {row_number}: negative {field_name}")
    error_status_code = result.get("error_status_code")
    if error_status_code is not None and not 100 <= error_status_code <= 599:
        raise ValueError(f"news row {row_number}: invalid error_status_code")
    if result["status"] == "ok" and error_status_code is not None:
        raise ValueError(f"news row {row_number}: ok row has error_status_code")
    if result["status"] == "ok" and result.get("news_id") != result["source_id"]:
        raise ValueError(f"news row {row_number}: ok row news_id differs from source_id")
    if result["status"] == "not_found" and error_status_code != 404:
        raise ValueError(f"news row {row_number}: not_found row must have status 404")
    if result["status"] == "parse_error" and not result["error_message"]:
        raise ValueError(f"news row {row_number}: parse_error has no error_message")

    reaction_total = sum(result[field_name] for field_name in REACTION_FIELDS)
    if result["reaction_total"] != reaction_total:
        raise ValueError(f"news row {row_number}: inconsistent reaction_total")
    loaded_count = result.get("comments_loaded_count")
    meta_count = result.get("comments_meta_count")
    if loaded_count is not None and meta_count is not None:
        if loaded_count > meta_count:
            raise ValueError(f"news row {row_number}: too many loaded comments")
        if result["comments_complete"] != (loaded_count == meta_count):
            raise ValueError(f"news row {row_number}: inconsistent comments_complete")
    return result


def iter_rows(
    input_path: Path,
    stats: RowStats,
    *,
    progress_every: int,
    expected_first_id: int | None = None,
    expected_last_id: int | None = None,
    require_contiguous_ids: bool = False,
) -> Iterator[dict[str, Any]]:
    with gzip.open(input_path, "rt", encoding="utf-8") as file:
        for row_number, line in enumerate(file, 1):
            try:
                source = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"news row {row_number}: invalid JSON") from error
            if not isinstance(source, dict):
                raise ValueError(f"news row {row_number}: expected JSON object")
            row = convert_row(source, row_number)
            if row_number == 1:
                stats.first_source_id = row["source_id"]
                if (
                    expected_first_id is not None
                    and row["source_id"] != expected_first_id
                ):
                    raise ValueError(
                        f"news row 1: expected source_id {expected_first_id}, "
                        f"got {row['source_id']}"
                    )
            if (
                stats.previous_source_id is not None
                and row["source_id"] <= stats.previous_source_id
            ):
                raise ValueError(
                    f"news row {row_number}: source_ids are not strictly increasing"
                )
            if (
                require_contiguous_ids
                and stats.previous_source_id is not None
                and row["source_id"] != stats.previous_source_id + 1
            ):
                raise ValueError(
                    f"news row {row_number}: source_ids are not contiguous"
                )
            stats.previous_source_id = row["source_id"]
            stats.row_count = row_number
            if progress_every and row_number % progress_every == 0:
                print(
                    f"news: prepared {row_number} rows",
                    file=sys.stderr,
                    flush=True,
                )
            yield row
    if stats.row_count and (
        expected_last_id is not None
        and stats.previous_source_id != expected_last_id
    ):
        raise ValueError(
            f"Last source_id differs: expected {expected_last_id}, "
            f"got {stats.previous_source_id}"
        )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def schema_sha256() -> str:
    serialized = json.dumps(
        table_schema(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _schema_signature(schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "name": str(column["name"]),
            "type": str(column["type"]),
            "required": bool(column.get("required", False)),
            **(
                {"sort_order": str(column["sort_order"])}
                if "sort_order" in column
                else {}
            ),
        }
        for column in schema
    ]


def verify_table(client: yt.YtClient, table_path: str, expected_count: int) -> None:
    row_count = int(client.get(table_path + "/@row_count"))
    if row_count != expected_count:
        raise RuntimeError(
            f"Final row count mismatch: expected {expected_count}, got {row_count}"
        )
    remote_schema = client.get(table_path + "/@schema")
    if _schema_signature(remote_schema) != _schema_signature(table_schema()):
        raise RuntimeError("YT table schema differs from the local schema")
    if not bool(client.get(table_path + "/@sorted")):
        raise RuntimeError("YT table is not sorted")
    sorted_by = [str(value) for value in client.get(table_path + "/@sorted_by")]
    if sorted_by != list(SORT_FIELDS):
        raise RuntimeError(
            f"YT sorted key mismatch: expected {list(SORT_FIELDS)}, got {sorted_by}"
        )


def append_batch(
    client: yt.YtClient,
    table_path: str,
    rows: list[dict[str, Any]],
    expected_before: int,
) -> int:
    before = int(client.get(table_path + "/@row_count"))
    if before != expected_before:
        raise RuntimeError(
            f"Stage row count changed: expected {expected_before}, got {before}"
        )
    expected_after = before + len(rows)
    try:
        client.write_table(
            yt.TablePath(table_path, append=True),
            rows,
            format=yt.YsonFormat(),
        )
    except Exception:
        # A transport error may arrive after the transaction was committed.
        after_error = int(client.get(table_path + "/@row_count"))
        if after_error != expected_after:
            raise
        return after_error
    after = int(client.get(table_path + "/@row_count"))
    if after != expected_after:
        raise RuntimeError(
            f"Batch row count mismatch: expected {expected_after}, got {after}"
        )
    return after


def read_boundary_ids(
    client: yt.YtClient, table_path: str, committed: int
) -> tuple[int, int]:
    if committed <= 0:
        raise ValueError("committed must be positive")
    first_rows = list(
        client.read_table(
            yt.TablePath(
                table_path,
                start_index=0,
                end_index=1,
                columns=["source_id"],
            ),
            format=yt.YsonFormat(),
        )
    )
    last_rows = list(
        client.read_table(
            yt.TablePath(
                table_path,
                start_index=committed - 1,
                end_index=committed,
                columns=["source_id"],
            ),
            format=yt.YsonFormat(),
        )
    )
    if len(first_rows) != 1 or len(last_rows) != 1:
        raise RuntimeError(
            f"Could not read staging boundary rows at checkpoint {committed}"
        )
    return int(first_rows[0]["source_id"]), int(last_rows[0]["source_id"])


def table_result(client: yt.YtClient, table_path: str, *, status: str) -> dict[str, Any]:
    return {
        "entity": ENTITY,
        "table": table_path,
        "status": status,
        "row_count": int(client.get(table_path + "/@row_count")),
        "column_count": len(FIELDS),
        "chunk_count": int(client.get(table_path + "/@chunk_count")),
        "data_weight": int(client.get(table_path + "/@data_weight")),
    }


def upload(
    client: yt.YtClient,
    input_dir: Path,
    table_path: str,
    *,
    force: bool,
    progress_every: int,
    batch_rows: int,
    allow_incomplete_source: bool = False,
    expected_min_id: int | None = DEFAULT_EXPECTED_MIN_ID,
    expected_max_id: int | None = DEFAULT_EXPECTED_MAX_ID,
) -> dict[str, Any]:
    manifest = load_manifest(
        input_dir,
        expected_min_id=expected_min_id,
        expected_max_id=expected_max_id,
    )
    if not manifest["source_parse_complete"] and not allow_incomplete_source:
        raise ValueError(
            "Source parsing is incomplete; inspect source_issue_details or use "
            "--allow-incomplete-source"
        )
    input_path = input_dir / INPUT_FILENAME
    if not input_path.is_file():
        raise ValueError(f"Input does not exist: {input_path}")

    expected_count = int(manifest["record_count"])
    expected_size = int(manifest["jsonl_size_bytes"])
    actual_size = input_path.stat().st_size
    if actual_size != expected_size:
        raise ValueError(
            f"Input size differs from manifest: expected {expected_size}, got {actual_size}"
        )
    source_digest = file_sha256(input_path)
    if source_digest != manifest["jsonl_sha256"]:
        raise ValueError("Input SHA-256 differs from manifest")
    schema_digest = schema_sha256()
    temporary_path = table_path + ".__uploading"

    if client.exists(table_path):
        same_source = (
            client.exists(table_path + "/@source_sha256")
            and str(client.get(table_path + "/@source_sha256")) == source_digest
            and client.exists(table_path + "/@schema_sha256")
            and str(client.get(table_path + "/@schema_sha256")) == schema_digest
            and int(client.get(table_path + "/@row_count")) == expected_count
        )
        if same_source:
            verify_table(client, table_path, expected_count)
            return table_result(client, table_path, status="already_present")
        if not force:
            raise ValueError(f"YT table already exists: {table_path}; use --force")

    stage_exists = client.exists(temporary_path)
    committed = 0
    checkpoint_id: int | None = None
    incompatibility: str | None = None
    if stage_exists:
        try:
            stage_source = str(client.get(temporary_path + "/@source_sha256"))
            stage_schema = str(client.get(temporary_path + "/@schema_sha256"))
            stage_expected = int(client.get(temporary_path + "/@expected_row_count"))
            if (
                stage_source != source_digest
                or stage_schema != schema_digest
                or stage_expected != expected_count
            ):
                incompatibility = "source, schema, or expected row count differs"
            remote_schema = client.get(temporary_path + "/@schema")
            if _schema_signature(remote_schema) != _schema_signature(table_schema()):
                incompatibility = "schema differs"
            committed = int(client.get(temporary_path + "/@row_count"))
            if committed > expected_count:
                incompatibility = "row count exceeds the manifest"
            elif committed:
                first_id, checkpoint_id = read_boundary_ids(
                    client, temporary_path, committed
                )
                if first_id != manifest["source_min_id"]:
                    incompatibility = "first source_id differs"
                elif manifest["source_ids_contiguous"] and checkpoint_id != (
                    manifest["source_min_id"] + committed - 1
                ):
                    incompatibility = "contiguous checkpoint source_id differs"
        except Exception as error:
            incompatibility = f"could not validate staging table: {error}"

        if incompatibility:
            if not force:
                raise RuntimeError(
                    f"Incompatible staging table exists: {temporary_path}: "
                    f"{incompatibility}"
                )
            client.remove(temporary_path, recursive=True, force=True)
            stage_exists = False
            committed = 0
            checkpoint_id = None

    if not stage_exists:
        client.create(
            "table",
            temporary_path,
            attributes={
                "schema": table_schema(),
                "entity": ENTITY,
                "source_sha256": source_digest,
                "schema_sha256": schema_digest,
                "expected_row_count": expected_count,
                "source_file": str(input_path.resolve()),
                "source_inventory_sha256": manifest["source_inventory_sha256"],
                "source_tree_sha256": manifest["source_tree_sha256"],
                "source_file_count": manifest["source_file_count"],
                "source_bytes": manifest["source_bytes"],
                "source_min_id": manifest["source_min_id"],
                "source_max_id": manifest["source_max_id"],
                "source_ids_contiguous": manifest["source_ids_contiguous"],
                "expected_min_id": manifest["expected_min_id"],
                "expected_max_id": manifest["expected_max_id"],
                "manifest_source_status_counts": manifest["source_status_counts"],
                "manifest_source_parse_complete": manifest["source_parse_complete"],
                "manifest_created_at": manifest["created_at"],
                "news_fields": list(FIELDS),
            },
        )
    if committed:
        print(
            f"news: resuming from {committed}/{expected_count} rows",
            file=sys.stderr,
            flush=True,
        )

    stats = RowStats()
    batch: list[dict[str, Any]] = []
    for row in iter_rows(
        input_path,
        stats,
        progress_every=progress_every,
        expected_first_id=manifest["source_min_id"],
        expected_last_id=manifest["source_max_id"],
        require_contiguous_ids=manifest["source_ids_contiguous"],
    ):
        if stats.row_count <= committed:
            if stats.row_count == committed and row["source_id"] != checkpoint_id:
                raise RuntimeError(
                    "Staging checkpoint differs from the local JSONL prefix"
                )
            continue
        batch.append(row)
        if len(batch) >= batch_rows:
            committed = append_batch(client, temporary_path, batch, committed)
            print(
                f"news: uploaded {committed}/{expected_count} rows",
                file=sys.stderr,
                flush=True,
            )
            batch = []
    if batch:
        committed = append_batch(client, temporary_path, batch, committed)
        print(
            f"news: uploaded {committed}/{expected_count} rows",
            file=sys.stderr,
            flush=True,
        )

    if stats.row_count != expected_count or committed != expected_count:
        raise RuntimeError(
            "Row count mismatch: "
            f"manifest={expected_count}, local={stats.row_count}, YT={committed}"
        )
    verify_table(client, temporary_path, expected_count)
    client.set(temporary_path + "/@upload_complete", True)
    client.move(temporary_path, table_path, force=force)
    verify_table(client, table_path, expected_count)
    return table_result(client, table_path, status="uploaded")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--proxy", default=os.environ.get("YT_PROXY", ""))
    parser.add_argument("--token", default=os.environ.get("YT_TOKEN") or None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-incomplete-source", action="store_true")
    parser.add_argument("--progress-every", type=int, default=1000)
    parser.add_argument("--batch-rows", type=int, default=500)
    parser.add_argument("--expected-min-id", type=int, default=DEFAULT_EXPECTED_MIN_ID)
    parser.add_argument("--expected-max-id", type=int, default=DEFAULT_EXPECTED_MAX_ID)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    if not args.proxy:
        raise SystemExit("YT proxy is required via --proxy or YT_PROXY")
    if args.progress_every < 0 or args.batch_rows <= 0:
        raise SystemExit("--progress-every must be non-negative and --batch-rows positive")

    client = yt.YtClient(
        proxy=args.proxy,
        token=args.token,
        config={
            "write_parallel": {
                "enable": True,
                "max_thread_count": 4,
                "unordered": False,
            },
            "write_retries": {"enable": True, "count": 8},
        },
    )
    parent = args.table.rsplit("/", 1)[0]
    if not client.exists(parent):
        client.create("map_node", parent, recursive=True)
    result = upload(
        client,
        args.input_dir.resolve(),
        args.table,
        force=args.force,
        progress_every=args.progress_every,
        batch_rows=args.batch_rows,
        allow_incomplete_source=args.allow_incomplete_source,
        expected_min_id=args.expected_min_id,
        expected_max_id=args.expected_max_id,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
