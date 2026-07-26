#!/usr/bin/env python3
"""Upload the extracted Shikimori TSV to a typed static YT table."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

import yt.wrapper as yt

from extract_shikimori import COLUMNS


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_TABLE = "//home/hc/ml-research/tmp-alexey.shishkov/shikimori_dump/anime"

BOOLEAN_COLUMNS = {
    "is_adult",
    "has_description",
}

FLOAT_COLUMNS = {
    "score",
    "best_rating",
    "score_distribution_mean",
}

INTEGER_COLUMNS = {
    "source_requested_id",
    "source_size_bytes",
    "canonical_id",
    "entry_id",
    "redirect_id",
    "discussion_topic_id",
    "episodes",
    "episodes_aired",
    "episodes_total",
    "duration_seconds",
    "score_votes_total",
    "user_list_total",
    "user_list_stats_total",
    "myanimelist_id",
    "anidb_id",
    "world_art_id",
    "anime_news_network_id",
    "kinopoisk_id",
    "poster_id",
    "poster_width",
    "poster_height",
    "poster_resolution_width",
    "poster_resolution_height",
    "video_duration_seconds",
} | {column for column in COLUMNS if column.endswith("_count")}


def column_type(column: str) -> str:
    if column in BOOLEAN_COLUMNS:
        return "boolean"
    if column in FLOAT_COLUMNS:
        return "double"
    if column in INTEGER_COLUMNS:
        return "int64"
    return "string"


def table_schema() -> list[dict[str, Any]]:
    return [
        {"name": column, "type": column_type(column), "required": False}
        for column in COLUMNS
    ]


def convert_value(column: str, value: str) -> Any:
    if column in BOOLEAN_COLUMNS:
        return bool(int(value))
    if column in INTEGER_COLUMNS:
        return int(value)
    if column in FLOAT_COLUMNS:
        return float(value)
    return value


def iter_rows(
    path: Path, status_counts: Counter[str], progress_every: int
) -> Iterator[dict[str, Any]]:
    csv.field_size_limit(sys.maxsize)
    with path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, delimiter="\t")
        if reader.fieldnames != COLUMNS:
            raise ValueError(
                f"Unexpected TSV schema: got {len(reader.fieldnames or [])} columns, "
                f"expected {len(COLUMNS)}"
            )
        for index, source_row in enumerate(reader, 1):
            row = {
                column: convert_value(column, value)
                for column, value in source_row.items()
                if value != ""
            }
            status_counts[source_row["page_status"]] += 1
            if progress_every and index % progress_every == 0:
                print(f"Prepared {index} rows", file=sys.stderr, flush=True)
            yield row


def upload(
    input_path: Path,
    table_path: str,
    *,
    proxy: str,
    token: str | None,
    force: bool,
    progress_every: int,
) -> dict[str, Any]:
    client = yt.YtClient(
        proxy=proxy,
        token=token,
        config={
            "write_parallel": {
                "enable": True,
                "max_thread_count": 4,
            },
        },
    )

    parent = table_path.rsplit("/", 1)[0]
    if not client.exists(parent):
        raise ValueError(f"YT parent does not exist: {parent}")
    if client.exists(table_path):
        if not force:
            raise ValueError(f"YT table already exists: {table_path}; use --force to replace it")
        client.remove(table_path, force=True)

    statuses: Counter[str] = Counter()
    destination = yt.TablePath(table_path, schema=table_schema())
    client.write_table(
        destination,
        iter_rows(input_path, statuses, progress_every),
        format=yt.YsonFormat(),
        force_create=True,
    )

    remote_row_count = int(client.get(table_path + "/@row_count"))
    remote_schema = client.get(table_path + "/@schema")
    local_row_count = sum(statuses.values())
    if remote_row_count != local_row_count:
        raise RuntimeError(
            f"Row count mismatch: local={local_row_count}, remote={remote_row_count}"
        )
    if len(remote_schema) != len(COLUMNS):
        raise RuntimeError(
            f"Schema mismatch: local={len(COLUMNS)}, remote={len(remote_schema)}"
        )

    client.set(table_path + "/@source_file", input_path.name)
    client.set(table_path + "/@page_status_counts", dict(statuses))
    client.set(table_path + "/@export_column_count", len(COLUMNS))
    return {
        "table": table_path,
        "row_count": remote_row_count,
        "column_count": len(remote_schema),
        "chunk_count": int(client.get(table_path + "/@chunk_count")),
        "data_weight": int(client.get(table_path + "/@data_weight")),
        "statuses": dict(statuses),
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=PROJECT_DIR / "shikimori.tsv",
    )
    parser.add_argument("table", nargs="?", default=DEFAULT_TABLE)
    parser.add_argument("--proxy", default=os.environ.get("YT_PROXY", ""))
    parser.add_argument("--token", default=os.environ.get("YT_TOKEN") or None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--progress-every", type=int, default=5000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    if not args.proxy:
        raise SystemExit("YT proxy is required via --proxy or YT_PROXY")
    result = upload(
        args.input.resolve(),
        args.table,
        proxy=args.proxy,
        token=args.token,
        force=args.force,
        progress_every=args.progress_every,
    )
    print(result, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
