#!/usr/bin/env python3
"""Upload parsed Shikimori reviews to a typed, sorted static YT table."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import yt.wrapper as yt


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = PROJECT_DIR / "reviews_parsed"
DEFAULT_TABLE = "//home/hc/ml-research/tmp-alexey.shishkov/shikimori_dump/reviews"

FIELDS = (
    "anime_id",
    "usefulness_rank",
    "review_id",
    "topic_id",
    "anime_slug",
    "anime_title",
    "anime_url",
    "review_url",
    "author_id",
    "author_nickname",
    "author_nickname_snapshot",
    "author_url",
    "author_avatar_url",
    "author_avatar_srcset",
    "opinion",
    "user_score",
    "user_list_status",
    "votes_for",
    "votes_against",
    "votes_total",
    "usefulness_score",
    "usefulness_ratio",
    "comments_count",
    "created_at",
    "published_at",
    "updated_at",
    "is_written_before_release",
    "body_text",
    "body_html",
    "body_links",
    "body_images",
    "inline_spoilers_count",
    "block_spoilers_count",
    "source_file",
    "source_page",
    "source_position",
)

SORT_FIELDS = ("anime_id", "usefulness_rank", "review_id")
RANKING_METHOD = "usefulness_score DESC, votes_for DESC, review_id DESC"
OPTIONAL_FIELDS = {"user_score", "user_list_status", "usefulness_ratio"}
INTEGER_FIELDS = {
    "anime_id",
    "usefulness_rank",
    "review_id",
    "topic_id",
    "author_id",
    "user_score",
    "votes_for",
    "votes_against",
    "votes_total",
    "usefulness_score",
    "comments_count",
    "inline_spoilers_count",
    "block_spoilers_count",
    "source_page",
    "source_position",
}
FLOAT_FIELDS = {"usefulness_ratio"}
BOOLEAN_FIELDS = {"is_written_before_release"}
ANY_FIELDS = {"body_links", "body_images"}

MANIFEST_ATTRIBUTE_KEYS = (
    "schema_version",
    "anime_count",
    "top_50_record_count",
    "source_directory_count",
    "source_file_count",
    "source_bytes",
    "source_status_counts",
    "source_pagination_complete",
    "source_incomplete_reasons",
    "source_issue_details",
    "terminal_postloader_pages",
    "declared_review_count_mismatch_anime_count",
    "source_tree_sha256_algorithm",
    "record_occurrence_count",
    "duplicate_count",
    "conflicting_duplicate_count",
    "jsonl",
    "jsonl_sha256",
    "jsonl_size_bytes",
    "ranking_method",
    "user_score_semantics",
    "extractor_sha256",
    "created_at",
)

SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass
class RowStats:
    row_count: int = 0
    previous_key: tuple[int, int, int] | None = None
    previous_anime_id: int | None = None
    previous_rank: int = 0
    previous_ranking_key: tuple[int, int, int] | None = None
    review_ids: set[int] = field(default_factory=set)


def field_type(field_name: str) -> str:
    if field_name in INTEGER_FIELDS:
        return "int64"
    if field_name in FLOAT_FIELDS:
        return "double"
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
            # YT's legacy schema does not allow required columns of type any.
            # convert_row still requires both structured body fields locally.
            "required": field_name not in OPTIONAL_FIELDS | ANY_FIELDS,
        }
        if field_name in SORT_FIELDS:
            column["sort_order"] = "ascending"
        schema.append(column)
    return schema


def load_manifest(input_dir: Path) -> dict[str, Any]:
    path = input_dir / "manifest.json"
    with path.open(encoding="utf-8") as file:
        manifest = json.load(file)
    if not isinstance(manifest, dict):
        raise ValueError(f"Invalid manifest: {path}")
    if manifest.get("complete") is not True:
        raise ValueError(f"Incomplete manifest: {path}")
    if manifest.get("entity") != "reviews":
        raise ValueError(f"Mismatched manifest entity: {manifest.get('entity')!r}")
    if manifest.get("extraction_complete") is not True:
        raise ValueError(f"Incomplete extraction manifest: {path}")
    schema_version = manifest.get("schema_version")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != 1
    ):
        raise ValueError(f"Unsupported manifest schema_version: {schema_version!r}")
    if manifest.get("fields") != list(FIELDS):
        raise ValueError("Manifest fields do not match the uploader schema")
    record_count = manifest.get("record_count")
    if (
        not isinstance(record_count, int)
        or isinstance(record_count, bool)
        or record_count < 0
    ):
        raise ValueError(f"Invalid manifest record_count: {record_count!r}")
    source_tree_sha256 = manifest.get("source_tree_sha256")
    if not isinstance(source_tree_sha256, str) or not SHA256_RE.fullmatch(
        source_tree_sha256
    ):
        raise ValueError(
            f"Invalid manifest source_tree_sha256: {source_tree_sha256!r}"
        )
    jsonl_sha256 = manifest.get("jsonl_sha256")
    if not isinstance(jsonl_sha256, str) or not SHA256_RE.fullmatch(jsonl_sha256):
        raise ValueError(f"Invalid manifest jsonl_sha256: {jsonl_sha256!r}")
    if not isinstance(manifest.get("source_pagination_complete"), bool):
        raise ValueError("Manifest source_pagination_complete must be boolean")
    ranking_method = manifest.get("ranking_method")
    if ranking_method != RANKING_METHOD:
        raise ValueError(
            f"Invalid manifest ranking_method: expected {RANKING_METHOD!r}, "
            f"got {ranking_method!r}"
        )
    return manifest


def _convert_integer(field_name: str, value: Any, row_number: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"reviews row {row_number}: invalid {field_name}")
    return value


def convert_row(source: dict[str, Any], row_number: int) -> dict[str, Any]:
    expected = set(FIELDS)
    actual = set(source)
    extra = actual - expected
    missing = expected - actual - OPTIONAL_FIELDS
    if missing or extra:
        raise ValueError(
            f"reviews row {row_number}: fields mismatch; "
            f"missing={sorted(missing)}, extra={sorted(extra)}"
        )

    result: dict[str, Any] = {}
    for field_name in FIELDS:
        value = source.get(field_name)
        if value is None:
            if field_name not in OPTIONAL_FIELDS:
                raise ValueError(f"reviews row {row_number}: null {field_name}")
            continue
        column_type = field_type(field_name)
        if column_type == "int64":
            result[field_name] = _convert_integer(field_name, value, row_number)
        elif column_type == "double":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"reviews row {row_number}: invalid {field_name}")
            result[field_name] = float(value)
        elif column_type == "boolean":
            if not isinstance(value, bool):
                raise ValueError(f"reviews row {row_number}: invalid {field_name}")
            result[field_name] = value
        elif column_type == "any":
            if not isinstance(value, list) or not all(
                isinstance(item, dict) for item in value
            ):
                raise ValueError(f"reviews row {row_number}: invalid {field_name}")
            result[field_name] = value
        else:
            if not isinstance(value, str):
                raise ValueError(f"reviews row {row_number}: invalid {field_name}")
            result[field_name] = value

    positive_fields = (
        "anime_id",
        "usefulness_rank",
        "review_id",
        "topic_id",
        "author_id",
        "source_page",
        "source_position",
    )
    if any(result[field_name] <= 0 for field_name in positive_fields):
        raise ValueError(f"reviews row {row_number}: non-positive identifier or position")
    nonnegative_fields = (
        "votes_for",
        "votes_against",
        "votes_total",
        "comments_count",
    )
    if any(result[field_name] < 0 for field_name in nonnegative_fields):
        raise ValueError(f"reviews row {row_number}: negative count")
    if result["opinion"] not in {"positive", "neutral", "negative"}:
        raise ValueError(f"reviews row {row_number}: invalid opinion")
    user_score = result.get("user_score")
    if user_score is not None and not 0 <= user_score <= 10:
        raise ValueError(f"reviews row {row_number}: invalid user_score")
    if result["inline_spoilers_count"] < 0 or result["block_spoilers_count"] < 0:
        raise ValueError(f"reviews row {row_number}: negative spoiler count")

    votes_total = result["votes_for"] + result["votes_against"]
    if result["votes_total"] != votes_total:
        raise ValueError(f"reviews row {row_number}: inconsistent votes_total")
    usefulness_score = result["votes_for"] - result["votes_against"]
    if result["usefulness_score"] != usefulness_score:
        raise ValueError(f"reviews row {row_number}: inconsistent usefulness_score")
    usefulness_ratio = result.get("usefulness_ratio")
    if votes_total == 0:
        if usefulness_ratio is not None:
            raise ValueError(
                f"reviews row {row_number}: usefulness_ratio set without votes"
            )
    elif usefulness_ratio is None or not math.isclose(
        usefulness_ratio,
        result["votes_for"] / votes_total,
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        raise ValueError(f"reviews row {row_number}: inconsistent usefulness_ratio")
    return result


def iter_rows(
    input_path: Path,
    stats: RowStats,
    *,
    progress_every: int,
) -> Iterator[dict[str, Any]]:
    with gzip.open(input_path, "rt", encoding="utf-8") as file:
        for row_number, line in enumerate(file, 1):
            try:
                source = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"reviews row {row_number}: invalid JSON") from error
            if not isinstance(source, dict):
                raise ValueError(f"reviews row {row_number}: expected JSON object")
            row = convert_row(source, row_number)
            key = tuple(row[field_name] for field_name in SORT_FIELDS)
            if stats.previous_key is not None and key <= stats.previous_key:
                raise ValueError(
                    f"reviews row {row_number}: keys are not strictly increasing"
                )
            anime_id = row["anime_id"]
            usefulness_rank = row["usefulness_rank"]
            expected_rank = (
                stats.previous_rank + 1
                if anime_id == stats.previous_anime_id
                else 1
            )
            if usefulness_rank != expected_rank:
                raise ValueError(
                    f"reviews row {row_number}: expected usefulness_rank "
                    f"{expected_rank}, got {usefulness_rank}"
                )
            ranking_key = (
                row["usefulness_score"],
                row["votes_for"],
                row["review_id"],
            )
            if (
                anime_id == stats.previous_anime_id
                and stats.previous_ranking_key is not None
                and ranking_key >= stats.previous_ranking_key
            ):
                raise ValueError(
                    f"reviews row {row_number}: rows do not follow {RANKING_METHOD}"
                )
            review_id = row["review_id"]
            if review_id in stats.review_ids:
                raise ValueError(f"reviews row {row_number}: duplicate review_id")
            stats.review_ids.add(review_id)
            stats.previous_key = key
            stats.previous_anime_id = anime_id
            stats.previous_rank = usefulness_rank
            stats.previous_ranking_key = ranking_key
            stats.row_count = row_number
            if progress_every and row_number % progress_every == 0:
                print(
                    f"reviews: prepared {row_number} rows",
                    file=sys.stderr,
                    flush=True,
                )
            yield row


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


def table_result(
    client: yt.YtClient, table_path: str, *, status: str
) -> dict[str, Any]:
    return {
        "entity": "reviews",
        "table": table_path,
        "status": status,
        "row_count": int(client.get(table_path + "/@row_count")),
        "column_count": len(FIELDS),
        "chunk_count": int(client.get(table_path + "/@chunk_count")),
        "data_weight": int(client.get(table_path + "/@data_weight")),
    }


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
        # A transport error can arrive after the transaction was committed.
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


def read_last_key(
    client: yt.YtClient, table_path: str, committed: int
) -> tuple[int, int, int]:
    if committed <= 0:
        raise ValueError("committed must be positive")
    rows = list(
        client.read_table(
            yt.TablePath(
                table_path,
                start_index=committed - 1,
                end_index=committed,
                columns=list(SORT_FIELDS),
            ),
            format=yt.YsonFormat(),
        )
    )
    if len(rows) != 1:
        raise RuntimeError(
            f"Could not read staging checkpoint row {committed}: got {len(rows)} rows"
        )
    try:
        return tuple(int(rows[0][field_name]) for field_name in SORT_FIELDS)
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("Invalid staging checkpoint row") from error


def upload(
    client: yt.YtClient,
    input_dir: Path,
    table_path: str,
    *,
    force: bool,
    progress_every: int,
    batch_rows: int,
    allow_incomplete_source: bool = False,
) -> dict[str, Any]:
    manifest = load_manifest(input_dir)
    if manifest.get("source_pagination_complete") is not True:
        if not allow_incomplete_source:
            raise ValueError(
                "Source pagination is incomplete; use --allow-incomplete-source "
                "to upload this snapshot explicitly"
            )
        print(
            "reviews: warning: uploading an incomplete source pagination snapshot",
            file=sys.stderr,
            flush=True,
        )
    input_path = input_dir / "reviews.jsonl.gz"
    if not input_path.is_file():
        raise ValueError(f"Input does not exist: {input_path}")

    expected_count = int(manifest["record_count"])
    source_digest = file_sha256(input_path)
    manifest_jsonl_digest = manifest["jsonl_sha256"]
    if manifest_jsonl_digest.lower() != source_digest:
        raise ValueError(
            "Manifest jsonl_sha256 does not match reviews.jsonl.gz"
        )
    schema_digest = schema_sha256()
    source_tree_digest = manifest["source_tree_sha256"].lower()
    temporary_path = f"{table_path}.__uploading"

    if client.exists(table_path):
        same_source = (
            client.exists(table_path + "/@source_sha256")
            and str(client.get(table_path + "/@source_sha256")) == source_digest
            and client.exists(table_path + "/@source_tree_sha256")
            and str(client.get(table_path + "/@source_tree_sha256")).lower()
            == source_tree_digest
            and client.exists(table_path + "/@schema_sha256")
            and str(client.get(table_path + "/@schema_sha256")) == schema_digest
            and int(client.get(table_path + "/@row_count")) == expected_count
        )
        if same_source:
            verify_table(client, table_path, expected_count)
            return table_result(client, table_path, status="already_present")
        if not force:
            raise ValueError(f"YT table already exists: {table_path}; use --force")

    if client.exists(temporary_path):
        stage_source = str(client.get(temporary_path + "/@source_sha256"))
        stage_tree = str(client.get(temporary_path + "/@source_tree_sha256")).lower()
        stage_schema = str(client.get(temporary_path + "/@schema_sha256"))
        stage_expected = int(client.get(temporary_path + "/@expected_row_count"))
        if (
            stage_source != source_digest
            or stage_tree != source_tree_digest
            or stage_schema != schema_digest
            or stage_expected != expected_count
        ):
            raise RuntimeError(f"Incompatible staging table exists: {temporary_path}")
    else:
        attributes: dict[str, Any] = {
            "schema": table_schema(),
            "source_sha256": source_digest,
            "source_tree_sha256": source_tree_digest,
            "schema_sha256": schema_digest,
            "expected_row_count": expected_count,
            "source_file": str(input_path.resolve()),
            "review_fields": list(FIELDS),
        }
        for key in MANIFEST_ATTRIBUTE_KEYS:
            if key in manifest:
                attributes[f"manifest_{key}"] = manifest[key]
        client.create("table", temporary_path, attributes=attributes)

    remote_schema = client.get(temporary_path + "/@schema")
    if _schema_signature(remote_schema) != _schema_signature(table_schema()):
        raise RuntimeError("Staging table schema differs from the local schema")

    committed = int(client.get(temporary_path + "/@row_count"))
    if committed > expected_count:
        raise RuntimeError(f"Staging table has too many rows: {committed}")
    if committed:
        print(
            f"reviews: resuming from {committed}/{expected_count} rows",
            file=sys.stderr,
            flush=True,
        )
    remote_last_key = (
        read_last_key(client, temporary_path, committed) if committed else None
    )

    stats = RowStats()
    batch: list[dict[str, Any]] = []
    for row in iter_rows(input_path, stats, progress_every=progress_every):
        if stats.row_count <= committed:
            if stats.row_count == committed:
                local_last_key = tuple(row[field_name] for field_name in SORT_FIELDS)
                if local_last_key != remote_last_key:
                    raise RuntimeError(
                        "Staging checkpoint key differs from the local input: "
                        f"remote={remote_last_key}, local={local_last_key}"
                    )
            continue
        batch.append(row)
        if len(batch) >= batch_rows:
            committed = append_batch(client, temporary_path, batch, committed)
            print(
                f"reviews: uploaded {committed}/{expected_count} rows",
                file=sys.stderr,
                flush=True,
            )
            batch = []
    if batch:
        committed = append_batch(client, temporary_path, batch, committed)
        print(
            f"reviews: uploaded {committed}/{expected_count} rows",
            file=sys.stderr,
            flush=True,
        )

    if stats.row_count != expected_count or committed != expected_count:
        raise RuntimeError(
            f"reviews: row count mismatch: manifest={expected_count}, "
            f"local={stats.row_count}, YT={committed}"
        )
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
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--batch-rows", type=int, default=1000)
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
        raise SystemExit(f"YT parent does not exist: {parent}")
    result = upload(
        client,
        args.input_dir.resolve(),
        args.table,
        force=args.force,
        progress_every=args.progress_every,
        batch_rows=args.batch_rows,
        allow_incomplete_source=args.allow_incomplete_source,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
