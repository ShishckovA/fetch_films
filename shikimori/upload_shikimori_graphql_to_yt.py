#!/usr/bin/env python3
"""Upload Shikimori GraphQL JSONL dumps to typed static YT tables."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import yt.wrapper as yt


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = PROJECT_DIR / "graphql"
DEFAULT_YT_DIR = "//home/hc/ml-research/tmp-alexey.shishkov/shikimori_dump"

ENTITY_FIELDS = {
    "animes": (
        "id",
        "malId",
        "name",
        "russian",
        "english",
        "japanese",
        "synonyms",
        "licenseNameRu",
        "licensors",
        "url",
        "kind",
        "status",
        "episodes",
        "episodesAired",
        "duration",
        "score",
        "rating",
        "origin",
        "franchise",
        "isCensored",
        "season",
        "createdAt",
        "updatedAt",
        "nextEpisodeAt",
        "opengraphImageUrl",
        "airedOn",
        "releasedOn",
        "poster",
        "genres",
        "studios",
        "description",
        "descriptionHtml",
        "descriptionSource",
        "fandubbers",
        "fansubbers",
        "externalLinks",
        "scoresStats",
        "statusesStats",
        "screenshots",
        "videos",
        "topic",
        "userRate",
        "related",
        "characterRoles",
        "personRoles",
    ),
    "characters": (
        "id",
        "malId",
        "name",
        "russian",
        "japanese",
        "synonyms",
        "description",
        "descriptionHtml",
        "descriptionSource",
        "isAnime",
        "isManga",
        "isRanobe",
        "createdAt",
        "updatedAt",
        "url",
        "poster",
        "topic",
    ),
    "people": (
        "id",
        "malId",
        "name",
        "russian",
        "japanese",
        "synonyms",
        "website",
        "isMangaka",
        "isProducer",
        "isSeyu",
        "createdAt",
        "updatedAt",
        "url",
        "birthOn",
        "deceasedOn",
        "poster",
        "topic",
    ),
}

INTEGER_FIELDS = {
    "animes": {"id", "malId", "episodes", "episodesAired", "duration"},
    "characters": {"id", "malId"},
    "people": {"id", "malId"},
}

FLOAT_FIELDS = {"animes": {"score"}, "characters": set(), "people": set()}

BOOLEAN_FIELDS = {
    "animes": {"isCensored"},
    "characters": {"isAnime", "isManga", "isRanobe"},
    "people": {"isMangaka", "isProducer", "isSeyu"},
}

ANY_FIELDS = {
    "animes": {
        "synonyms",
        "licensors",
        "airedOn",
        "releasedOn",
        "poster",
        "genres",
        "studios",
        "fandubbers",
        "fansubbers",
        "externalLinks",
        "scoresStats",
        "statusesStats",
        "screenshots",
        "videos",
        "topic",
        "userRate",
        "related",
        "characterRoles",
        "personRoles",
    },
    "characters": {"synonyms", "poster", "topic"},
    "people": {"synonyms", "birthOn", "deceasedOn", "poster", "topic"},
}


@dataclass
class RowStats:
    row_count: int = 0
    previous_id: int = -1


def field_type(entity: str, field: str) -> str:
    if field in INTEGER_FIELDS[entity]:
        return "int64"
    if field in FLOAT_FIELDS[entity]:
        return "double"
    if field in BOOLEAN_FIELDS[entity]:
        return "boolean"
    if field in ANY_FIELDS[entity]:
        return "any"
    return "string"


def table_schema(entity: str) -> list[dict[str, Any]]:
    schema = []
    for field in ENTITY_FIELDS[entity]:
        column: dict[str, Any] = {
            "name": field,
            "type": field_type(entity, field),
            "required": field == "id",
        }
        if field == "id":
            column["sort_order"] = "ascending"
        schema.append(column)
    return schema


def load_manifest(input_dir: Path, entity: str) -> dict[str, Any]:
    path = input_dir / entity / "manifest.json"
    with path.open(encoding="utf-8") as file:
        manifest = json.load(file)
    if not isinstance(manifest, dict):
        raise ValueError(f"Invalid manifest: {path}")
    if manifest.get("entity") != entity or manifest.get("complete") is not True:
        raise ValueError(f"Incomplete or mismatched manifest: {path}")
    if not isinstance(manifest.get("record_count"), int):
        raise ValueError(f"Manifest has no record_count: {path}")
    return manifest


def convert_row(entity: str, source: dict[str, Any], row_number: int) -> dict[str, Any]:
    fields = ENTITY_FIELDS[entity]
    expected = set(fields)
    actual = set(source)
    if actual != expected:
        raise ValueError(
            f"{entity} row {row_number}: fields mismatch; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )

    result = {}
    for field in fields:
        value = source[field]
        if value is None:
            if field == "id":
                raise ValueError(f"{entity} row {row_number}: null id")
            continue
        column_type = field_type(entity, field)
        if column_type == "int64":
            if isinstance(value, bool):
                raise ValueError(f"{entity} row {row_number}: invalid {field}")
            result[field] = int(value)
        elif column_type == "double":
            if isinstance(value, bool):
                raise ValueError(f"{entity} row {row_number}: invalid {field}")
            result[field] = float(value)
        elif column_type == "boolean":
            if not isinstance(value, bool):
                raise ValueError(f"{entity} row {row_number}: invalid {field}")
            result[field] = value
        elif column_type == "any":
            if not isinstance(value, (dict, list)):
                raise ValueError(f"{entity} row {row_number}: invalid {field}")
            result[field] = value
        else:
            if not isinstance(value, str):
                raise ValueError(f"{entity} row {row_number}: invalid {field}")
            result[field] = value
    return result


def iter_rows(
    input_path: Path,
    entity: str,
    stats: RowStats,
    *,
    progress_every: int,
) -> Iterator[dict[str, Any]]:
    with gzip.open(input_path, "rt", encoding="utf-8") as file:
        for row_number, line in enumerate(file, 1):
            source = json.loads(line)
            if not isinstance(source, dict):
                raise ValueError(f"{entity} row {row_number}: expected JSON object")
            row = convert_row(entity, source, row_number)
            row_id = row["id"]
            if row_id <= stats.previous_id:
                raise ValueError(
                    f"{entity} row {row_number}: ids are not strictly increasing"
                )
            stats.previous_id = row_id
            stats.row_count = row_number
            if progress_every and row_number % progress_every == 0:
                print(
                    f"{entity}: prepared {row_number} rows",
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


def schema_sha256(entity: str) -> str:
    serialized = json.dumps(
        table_schema(entity),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def table_result(
    client: yt.YtClient, entity: str, table_path: str, *, status: str
) -> dict[str, Any]:
    return {
        "entity": entity,
        "table": table_path,
        "status": status,
        "row_count": int(client.get(table_path + "/@row_count")),
        "column_count": len(ENTITY_FIELDS[entity]),
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


def upload_entity(
    client: yt.YtClient,
    input_dir: Path,
    yt_dir: str,
    entity: str,
    *,
    force: bool,
    progress_every: int,
    batch_rows: int,
) -> dict[str, Any]:
    manifest = load_manifest(input_dir, entity)
    input_path = input_dir / entity / f"{entity}.jsonl.gz"
    if not input_path.is_file():
        raise ValueError(f"Input does not exist: {input_path}")

    table_path = f"{yt_dir.rstrip('/')}/{entity}"
    temporary_path = f"{table_path}.__uploading"
    expected_count = int(manifest["record_count"])
    source_digest = file_sha256(input_path)
    schema_digest = schema_sha256(entity)

    if client.exists(table_path):
        same_source = (
            client.exists(table_path + "/@source_sha256")
            and str(client.get(table_path + "/@source_sha256")) == source_digest
            and int(client.get(table_path + "/@row_count")) == expected_count
        )
        if same_source:
            return table_result(client, entity, table_path, status="already_present")
        if not force:
            raise ValueError(f"YT table already exists: {table_path}; use --force")

    if client.exists(temporary_path):
        stage_source = str(client.get(temporary_path + "/@source_sha256"))
        stage_schema = str(client.get(temporary_path + "/@schema_sha256"))
        stage_expected = int(client.get(temporary_path + "/@expected_row_count"))
        if (
            stage_source != source_digest
            or stage_schema != schema_digest
            or stage_expected != expected_count
        ):
            raise RuntimeError(
                f"Incompatible staging table exists: {temporary_path}"
            )
    else:
        client.create(
            "table",
            temporary_path,
            attributes={
                "schema": table_schema(entity),
                "source_sha256": source_digest,
                "schema_sha256": schema_digest,
                "expected_row_count": expected_count,
                "source_file": str(input_path.resolve()),
                "source_endpoint": manifest.get("endpoint"),
                "source_selection_sha256": manifest.get("selection_sha256"),
                "graphql_fields": list(ENTITY_FIELDS[entity]),
            },
        )

    remote_schema = client.get(temporary_path + "/@schema")
    remote_names = [str(column["name"]) for column in remote_schema]
    if remote_names != list(ENTITY_FIELDS[entity]):
        raise RuntimeError(f"{entity}: staging schema columns differ")

    committed = int(client.get(temporary_path + "/@row_count"))
    if committed > expected_count:
        raise RuntimeError(f"{entity}: staging table has too many rows: {committed}")
    if committed:
        print(
            f"{entity}: resuming from {committed}/{expected_count} rows",
            file=sys.stderr,
            flush=True,
        )

    stats = RowStats()
    batch: list[dict[str, Any]] = []
    for row in iter_rows(
        input_path,
        entity,
        stats,
        progress_every=progress_every,
    ):
        if stats.row_count <= committed:
            continue
        batch.append(row)
        if len(batch) >= batch_rows:
            committed = append_batch(client, temporary_path, batch, committed)
            print(
                f"{entity}: uploaded {committed}/{expected_count} rows",
                file=sys.stderr,
                flush=True,
            )
            batch = []
    if batch:
        committed = append_batch(client, temporary_path, batch, committed)
        print(
            f"{entity}: uploaded {committed}/{expected_count} rows",
            file=sys.stderr,
            flush=True,
        )

    if stats.row_count != expected_count or committed != expected_count:
        raise RuntimeError(
            f"{entity}: row count mismatch: manifest={expected_count}, "
            f"local={stats.row_count}, YT={committed}"
        )
    client.set(temporary_path + "/@upload_complete", True)
    client.move(temporary_path, table_path, force=force)
    result = table_result(client, entity, table_path, status="uploaded")
    if result["row_count"] != expected_count:
        raise RuntimeError(
            f"{entity}: final row count mismatch: "
            f"{result['row_count']} != {expected_count}"
        )
    return result


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--yt-dir", default=DEFAULT_YT_DIR)
    parser.add_argument(
        "--entities",
        nargs="+",
        choices=tuple(ENTITY_FIELDS),
        default=list(ENTITY_FIELDS),
    )
    parser.add_argument("--proxy", default=os.environ.get("YT_PROXY", ""))
    parser.add_argument("--token", default=os.environ.get("YT_TOKEN") or None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--batch-rows", type=int, default=2000)
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
    if not client.exists(args.yt_dir):
        raise SystemExit(f"YT directory does not exist: {args.yt_dir}")

    results = []
    for entity in args.entities:
        result = upload_entity(
            client,
            args.input_dir.resolve(),
            args.yt_dir,
            entity,
            force=args.force,
            progress_every=args.progress_every,
            batch_rows=args.batch_rows,
        )
        results.append(result)
        print(json.dumps(result, ensure_ascii=False), file=sys.stderr, flush=True)
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
