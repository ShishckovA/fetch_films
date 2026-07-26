#!/usr/bin/env python3
"""Upload raw HTML snapshots to resumable YTsaurus tables.

Each destination table has three columns:

* ``url``  — canonical page URL and ascending sort key;
* ``html`` — original file bytes, without parsing or normalization;
* ``meta`` — provenance, content digest, size, page kind, and validation status.

Uploads use a ``pages.__uploading`` staging table and can resume from its
remote row count.  The completed table is moved atomically to ``pages``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_YT_DIR = (
    "//home/hc/ml-research/tmp-alexey.shishkov/film_data"
)
DATASET_NAMES = ("afisha", "bolshoy_vopros", "film")
SCHEMA_VERSION = 1
DEFAULT_BATCH_ROWS = 500
DEFAULT_BATCH_BYTES = 64 * 1024 * 1024
BV_FILE_RE = re.compile(r"^[0-9a-f]{16}_(.+\.html)$")
AFISHA_SBER_ID_MARKER = "<title>Сбер ID</title>".encode("utf-8")


@dataclass(frozen=True)
class PageEntry:
    dataset: str
    url: str
    path: Path
    source_file: str
    page_kind: str
    sources: tuple[str, ...]
    original_url: str | None = None
    validation_status: str = "ok"


def table_schema() -> list[dict[str, Any]]:
    return [
        {
            "name": "url",
            "type": "string",
            "required": True,
            "sort_order": "ascending",
        },
        {"name": "html", "type": "string", "required": True},
        # Legacy YT schemas do not allow required columns of type any.
        {"name": "meta", "type": "any", "required": False},
    ]


def schema_sha256() -> str:
    serialized = json.dumps(
        table_schema(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def relative_source_file(path: Path) -> str:
    return path.resolve().relative_to(PROJECT_DIR).as_posix()


def read_nonempty_lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def afisha_page_filename(url: str) -> str:
    parsed = urlsplit(url)
    flattened = parsed.path.strip("/").replace("/", "-")
    safe_name = "".join(
        character
        if character.isalnum() or character in "-_."
        else "-"
        for character in flattened
    )
    return f"{safe_name}.html"


def inventory_afisha() -> list[PageEntry]:
    pages_dir = PROJECT_DIR / "afisha" / "selections copy"
    urls_path = PROJECT_DIR / "afisha" / "selection_urls.txt"
    urls = read_nonempty_lines(urls_path)

    entries: list[PageEntry] = []
    selected_names: set[str] = set()
    for url in urls:
        filename = afisha_page_filename(url)
        path = pages_dir / filename
        if not path.is_file():
            raise FileNotFoundError(
                f"Afisha page for {url} not found: {path}"
            )
        selected_names.add(filename)
        prefix = path.read_bytes()[:32 * 1024]
        validation_status = (
            "unexpected_sber_id"
            if AFISHA_SBER_ID_MARKER in prefix
            else "ok"
        )
        entries.append(
            PageEntry(
                dataset="afisha",
                url=url,
                path=path,
                source_file=relative_source_file(path),
                page_kind="selection",
                sources=(urls_path.name,),
                validation_status=validation_status,
            )
        )

    actual_names = {path.name for path in pages_dir.glob("*.html")}
    if actual_names != selected_names:
        missing = sorted(selected_names - actual_names)
        extra = sorted(actual_names - selected_names)
        raise RuntimeError(
            "Afisha URL/file inventory mismatch: "
            f"missing={missing[:5]}, extra={extra[:5]}"
        )
    return validate_entries(entries, "afisha")


def normalize_bolshoy_url(raw_url: str) -> str:
    parsed = urlsplit(raw_url)
    if parsed.scheme and parsed.netloc:
        path = "/" + parsed.path.lstrip("/")
    else:
        path = "/" + raw_url.lstrip("/")
    return urlunsplit(
        ("https", "www.bolshoyvopros.ru", path, "", "")
    )


def inventory_bolshoy_vopros() -> list[PageEntry]:
    project_dir = PROJECT_DIR / "bolshoy_vopros"
    pages_dir = project_dir / "tag_238_728_html" / "pages"
    source_lists = (
        project_dir / "tag_238.txt",
        project_dir / "tag_728.txt",
    )

    sources_by_basename: dict[str, list[str]] = {}
    url_by_basename: dict[str, str] = {}
    original_by_basename: dict[str, str] = {}
    for source_list in source_lists:
        for raw_url in read_nonempty_lines(source_list):
            canonical = normalize_bolshoy_url(raw_url)
            basename = Path(urlsplit(canonical).path).name
            if not basename:
                raise ValueError(
                    f"Bolshoy Vopros URL has no basename: {raw_url}"
                )
            previous = url_by_basename.setdefault(basename, canonical)
            if previous != canonical:
                raise ValueError(
                    f"Conflicting URLs for basename {basename}: "
                    f"{previous}, {canonical}"
                )
            original_by_basename.setdefault(basename, raw_url)
            source_names = sources_by_basename.setdefault(basename, [])
            if source_list.name not in source_names:
                source_names.append(source_list.name)

    page_by_basename: dict[str, Path] = {}
    for path in pages_dir.glob("*.html"):
        match = BV_FILE_RE.match(path.name)
        if not match:
            raise ValueError(
                f"Unexpected Bolshoy Vopros filename: {path.name}"
            )
        basename = match.group(1)
        if basename in page_by_basename:
            raise ValueError(
                f"Duplicate Bolshoy Vopros page basename: {basename}"
            )
        page_by_basename[basename] = path

    expected = set(url_by_basename)
    actual = set(page_by_basename)
    if expected != actual:
        raise RuntimeError(
            "Bolshoy Vopros URL/file inventory mismatch: "
            f"missing={sorted(expected - actual)[:5]}, "
            f"extra={sorted(actual - expected)[:5]}"
        )

    entries = []
    for basename, url in url_by_basename.items():
        path = page_by_basename[basename]
        original = original_by_basename[basename]
        entries.append(
            PageEntry(
                dataset="bolshoy_vopros",
                url=url,
                path=path,
                source_file=relative_source_file(path),
                page_kind="question",
                sources=tuple(sources_by_basename[basename]),
                original_url=original if original != url else None,
            )
        )
    return validate_entries(entries, "bolshoy_vopros")


def load_tsv_manifest(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    expected_fields = {"filename", "url", "sources"}
    if not rows or not expected_fields.issubset(rows[0]):
        raise ValueError(f"Unexpected manifest schema: {path}")
    return rows


def inventory_film() -> list[PageEntry]:
    pages_dir = PROJECT_DIR / "film" / "fetched 2"
    manifest_path = pages_dir / "manifest.tsv"
    manifest_rows = load_tsv_manifest(manifest_path)

    entries: list[PageEntry] = []
    manifest_names: set[str] = set()
    for manifest_row in manifest_rows:
        filename = manifest_row["filename"]
        path = pages_dir / filename
        if not path.is_file():
            raise FileNotFoundError(f"Film.ru page not found: {path}")
        if filename in manifest_names:
            raise ValueError(f"Duplicate Film.ru manifest filename: {filename}")
        manifest_names.add(filename)

        url = manifest_row["url"].strip()
        url_path = urlsplit(url).path
        if url_path.startswith("/news/"):
            page_kind = "news"
        elif url_path.startswith("/articles/"):
            page_kind = "article"
        else:
            raise ValueError(f"Unexpected Film.ru content URL: {url}")

        sources = tuple(
            value
            for value in manifest_row["sources"].split(",")
            if value
        )
        entries.append(
            PageEntry(
                dataset="film",
                url=url,
                path=path,
                source_file=relative_source_file(path),
                page_kind=page_kind,
                sources=sources,
            )
        )

    actual_names = {path.name for path in pages_dir.glob("*.html")}
    if actual_names != manifest_names:
        raise RuntimeError(
            "Film.ru manifest/file inventory mismatch: "
            f"missing={sorted(manifest_names - actual_names)[:5]}, "
            f"extra={sorted(actual_names - manifest_names)[:5]}"
        )
    return validate_entries(entries, "film")


def validate_entries(
    entries: Iterable[PageEntry], dataset: str
) -> list[PageEntry]:
    ordered = sorted(entries, key=lambda entry: entry.url)
    if not ordered:
        raise ValueError(f"No source pages found for {dataset}")

    previous_url: str | None = None
    for entry in ordered:
        if entry.dataset != dataset:
            raise ValueError(
                f"Dataset mismatch: expected {dataset}, got {entry.dataset}"
            )
        if entry.url == previous_url:
            raise ValueError(f"Duplicate {dataset} URL: {entry.url}")
        previous_url = entry.url
        if not entry.path.is_file() or entry.path.stat().st_size <= 0:
            raise ValueError(f"Empty or missing source page: {entry.path}")
    return ordered


def build_inventory(dataset: str) -> list[PageEntry]:
    builders = {
        "afisha": inventory_afisha,
        "bolshoy_vopros": inventory_bolshoy_vopros,
        "film": inventory_film,
    }
    return builders[dataset]()


def source_tree_fingerprint(entries: list[PageEntry]) -> str:
    digest = hashlib.sha256()
    for entry in entries:
        stat = entry.path.stat()
        row = (
            f"{entry.url}\0{entry.source_file}\0"
            f"{stat.st_size}\0{stat.st_mtime_ns}\n"
        )
        digest.update(row.encode("utf-8"))
    return digest.hexdigest()


def source_bytes(entries: list[PageEntry]) -> int:
    return sum(entry.path.stat().st_size for entry in entries)


def entry_to_row(entry: PageEntry) -> dict[str, Any]:
    html = entry.path.read_bytes()
    stat = entry.path.stat()
    meta: dict[str, Any] = {
        "dataset": entry.dataset,
        "source_file": entry.source_file,
        "page_kind": entry.page_kind,
        "content_type": "text/html",
        "content_bytes": len(html),
        "content_sha256": hashlib.sha256(html).hexdigest(),
        "file_mtime_utc": datetime.fromtimestamp(
            stat.st_mtime, tz=timezone.utc
        ).isoformat(),
        "sources": list(entry.sources),
        "validation_status": entry.validation_status,
    }
    if entry.original_url is not None:
        meta["original_url"] = entry.original_url
    return {"url": entry.url, "html": html, "meta": meta}


def schema_signature(
    schema: list[dict[str, Any]],
) -> list[dict[str, Any]]:
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


def append_batch(
    yt: Any,
    client: Any,
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
        # A transport error can arrive after the append transaction commits.
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


def read_checkpoint_url(
    yt: Any, client: Any, table_path: str, committed: int
) -> str:
    rows = list(
        client.read_table(
            yt.TablePath(
                table_path,
                start_index=committed - 1,
                end_index=committed,
                columns=["url"],
            ),
            format=yt.YsonFormat(),
        )
    )
    if len(rows) != 1 or "url" not in rows[0]:
        raise RuntimeError(
            f"Could not read checkpoint row {committed} from {table_path}"
        )
    value = rows[0]["url"]
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def verify_table(
    client: Any,
    table_path: str,
    expected_count: int,
    expected_fingerprint: str,
) -> None:
    row_count = int(client.get(table_path + "/@row_count"))
    if row_count != expected_count:
        raise RuntimeError(
            f"{table_path}: expected {expected_count} rows, got {row_count}"
        )
    if (
        str(client.get(table_path + "/@source_tree_fingerprint"))
        != expected_fingerprint
    ):
        raise RuntimeError(f"{table_path}: source fingerprint mismatch")
    remote_schema = client.get(table_path + "/@schema")
    if schema_signature(remote_schema) != schema_signature(table_schema()):
        raise RuntimeError(f"{table_path}: schema mismatch")
    if not bool(client.get(table_path + "/@sorted")):
        raise RuntimeError(f"{table_path}: table is not sorted")
    if [str(item) for item in client.get(table_path + "/@sorted_by")] != [
        "url"
    ]:
        raise RuntimeError(f"{table_path}: unexpected sort key")


def table_result(
    client: Any, table_path: str, dataset: str, status: str
) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "table": table_path,
        "status": status,
        "row_count": int(client.get(table_path + "/@row_count")),
        "chunk_count": int(client.get(table_path + "/@chunk_count")),
        "data_weight": int(client.get(table_path + "/@data_weight")),
        "compressed_data_size": int(
            client.get(table_path + "/@compressed_data_size")
        ),
    }


def upload_dataset(
    yt: Any,
    client: Any,
    yt_dir: str,
    dataset: str,
    entries: list[PageEntry],
    *,
    batch_rows: int,
    batch_bytes: int,
    force: bool,
) -> dict[str, Any]:
    parent = f"{yt_dir.rstrip('/')}/{dataset}"
    table_path = f"{parent}/pages"
    stage_path = f"{table_path}.__uploading"
    expected_count = len(entries)
    expected_source_bytes = source_bytes(entries)
    fingerprint = source_tree_fingerprint(entries)
    schema_digest = schema_sha256()

    if not client.exists(parent):
        client.create("map_node", parent, recursive=True)

    if client.exists(table_path):
        same_source = (
            client.exists(table_path + "/@source_tree_fingerprint")
            and str(
                client.get(table_path + "/@source_tree_fingerprint")
            )
            == fingerprint
            and client.exists(table_path + "/@schema_sha256")
            and str(client.get(table_path + "/@schema_sha256"))
            == schema_digest
            and int(client.get(table_path + "/@row_count"))
            == expected_count
        )
        if same_source:
            verify_table(
                client, table_path, expected_count, fingerprint
            )
            return table_result(
                client, table_path, dataset, "already_present"
            )
        if not force:
            raise RuntimeError(
                f"YT table already exists with different source: {table_path}; "
                "use --force only after reviewing it"
            )

    compatible_stage = False
    if client.exists(stage_path):
        compatible_stage = (
            str(
                client.get(stage_path + "/@source_tree_fingerprint")
            )
            == fingerprint
            and str(client.get(stage_path + "/@schema_sha256"))
            == schema_digest
            and int(client.get(stage_path + "/@expected_row_count"))
            == expected_count
        )
        if not compatible_stage:
            raise RuntimeError(
                f"Incompatible staging table exists: {stage_path}"
            )
    else:
        attributes = {
            "schema": table_schema(),
            "schema_version": SCHEMA_VERSION,
            "schema_sha256": schema_digest,
            "dataset": dataset,
            "description": (
                f"Raw, unparsed HTML pages for {dataset}; "
                "per-row provenance is stored in meta"
            ),
            "html_is_raw": True,
            "expected_row_count": expected_count,
            "expected_source_bytes": expected_source_bytes,
            "source_tree_fingerprint": fingerprint,
            "source_tree_fingerprint_algorithm": (
                "sha256(url,source_file,size,mtime_ns)"
            ),
            "compression_codec": "zstd_3",
            "optimize_for": "scan",
        }
        client.create("table", stage_path, attributes=attributes)

    remote_schema = client.get(stage_path + "/@schema")
    if schema_signature(remote_schema) != schema_signature(table_schema()):
        raise RuntimeError(f"{stage_path}: staging schema mismatch")

    committed = int(client.get(stage_path + "/@row_count"))
    if committed > expected_count:
        raise RuntimeError(
            f"{stage_path}: too many staged rows: {committed}"
        )
    if committed:
        remote_url = read_checkpoint_url(
            yt, client, stage_path, committed
        )
        local_url = entries[committed - 1].url
        if remote_url != local_url:
            raise RuntimeError(
                f"{stage_path}: checkpoint mismatch: "
                f"remote={remote_url!r}, local={local_url!r}"
            )
        print(
            f"{dataset}: resuming from {committed}/{expected_count}",
            file=sys.stderr,
            flush=True,
        )

    batch: list[dict[str, Any]] = []
    batch_weight = 0
    for entry in entries[committed:]:
        row = entry_to_row(entry)
        row_weight = len(row["html"])
        if batch and (
            len(batch) >= batch_rows
            or batch_weight + row_weight > batch_bytes
        ):
            committed = append_batch(
                yt, client, stage_path, batch, committed
            )
            print(
                f"{dataset}: uploaded {committed}/{expected_count} rows",
                file=sys.stderr,
                flush=True,
            )
            batch = []
            batch_weight = 0
        batch.append(row)
        batch_weight += row_weight

    if batch:
        committed = append_batch(
            yt, client, stage_path, batch, committed
        )
        print(
            f"{dataset}: uploaded {committed}/{expected_count} rows",
            file=sys.stderr,
            flush=True,
        )

    if committed != expected_count:
        raise RuntimeError(
            f"{dataset}: upload incomplete: "
            f"{committed}/{expected_count} rows"
        )
    client.set(stage_path + "/@upload_complete", True)
    client.set(
        stage_path + "/@uploaded_at",
        datetime.now(timezone.utc).isoformat(),
    )
    client.move(stage_path, table_path, force=force)
    verify_table(client, table_path, expected_count, fingerprint)
    return table_result(client, table_path, dataset, "uploaded")


def dry_run_result(
    dataset: str, entries: list[PageEntry]
) -> dict[str, Any]:
    statuses: dict[str, int] = {}
    for entry in entries:
        statuses[entry.validation_status] = (
            statuses.get(entry.validation_status, 0) + 1
        )
    return {
        "dataset": dataset,
        "row_count": len(entries),
        "source_bytes": source_bytes(entries),
        "source_tree_fingerprint": source_tree_fingerprint(entries),
        "validation_status_counts": statuses,
        "first_url": entries[0].url,
        "last_url": entries[-1].url,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=DATASET_NAMES,
        help="dataset to upload; repeat as needed (default: all)",
    )
    parser.add_argument("--yt-dir", default=DEFAULT_YT_DIR)
    parser.add_argument(
        "--proxy", default=os.environ.get("YT_PROXY", "")
    )
    parser.add_argument(
        "--token", default=os.environ.get("YT_TOKEN") or None
    )
    parser.add_argument(
        "--batch-rows", type=int, default=DEFAULT_BATCH_ROWS
    )
    parser.add_argument(
        "--batch-bytes", type=int, default=DEFAULT_BATCH_BYTES
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    if args.batch_rows <= 0 or args.batch_bytes <= 0:
        raise SystemExit("--batch-rows and --batch-bytes must be positive")

    datasets = args.dataset or list(DATASET_NAMES)
    inventories = {
        dataset: build_inventory(dataset) for dataset in datasets
    }
    if args.dry_run:
        print(
            json.dumps(
                [
                    dry_run_result(dataset, inventories[dataset])
                    for dataset in datasets
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if not args.proxy:
        raise SystemExit("YT proxy is required via --proxy or YT_PROXY")

    from yt import wrapper as yt

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
        raise SystemExit(f"YT destination does not exist: {args.yt_dir}")

    results = []
    for dataset in datasets:
        results.append(
            upload_dataset(
                yt,
                client,
                args.yt_dir,
                dataset,
                inventories[dataset],
                batch_rows=args.batch_rows,
                batch_bytes=args.batch_bytes,
                force=args.force,
            )
        )
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
