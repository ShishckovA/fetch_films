#!/usr/bin/env python3
"""Atomically move the complete Shikimori YT subtree under film_data."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any

import yt.wrapper as yt


DEFAULT_SOURCE = "//home/hc/ml-research/tmp-alexey.shishkov/shikimori_dump"
DEFAULT_TARGET = (
    "//home/hc/ml-research/tmp-alexey.shishkov/film_data/shikimori"
)
EXPECTED_SHIKIMORI_CHILDREN = frozenset(
    {"anime", "anime-useful", "animes", "characters", "people", "reviews"}
)


@dataclass(frozen=True)
class SubtreeInventory:
    node_id: str
    node_type: str
    children: dict[str, str]


def _normalize_path(path: str) -> str:
    normalized = path.rstrip("/")
    if not normalized.startswith("//") or normalized == "//":
        raise ValueError(f"Expected an absolute non-root YT path, got {path!r}")
    return normalized


def _parent_path(path: str) -> str:
    parent, separator, _ = path.rpartition("/")
    if not separator or not parent:
        raise ValueError(f"YT path has no parent: {path!r}")
    return parent


def inspect_subtree(client: yt.YtClient, path: str) -> SubtreeInventory:
    node_type = str(client.get(path + "/@type"))
    if node_type != "map_node":
        raise ValueError(f"Expected a map_node at {path}, got {node_type!r}")

    child_names = sorted(str(name) for name in client.list(path))
    return SubtreeInventory(
        node_id=str(client.get(path + "/@id")),
        node_type=node_type,
        children={
            name: str(client.get(f"{path}/{name}/@id")) for name in child_names
        },
    )


def move_subtree(
    client: yt.YtClient,
    source: str,
    target: str,
    *,
    dry_run: bool = False,
    expected_children: set[str] | frozenset[str] | None = None,
) -> dict[str, Any]:
    source = _normalize_path(source)
    target = _normalize_path(target)
    if source == target:
        raise ValueError("Source and target paths are identical")
    if target.startswith(source + "/") or source.startswith(target + "/"):
        raise ValueError("Source and target must not contain one another")
    source_exists = client.exists(source)
    target_exists = client.exists(target)
    if source_exists and target_exists:
        raise ValueError(f"Target YT node already exists: {target}")
    if not source_exists and not target_exists:
        raise ValueError(
            f"Neither source nor target YT node exists: {source}, {target}"
        )
    if not source_exists:
        after = inspect_subtree(client, target)
        actual_children = set(after.children)
        if expected_children is not None and actual_children != set(expected_children):
            raise RuntimeError(
                f"Existing target children differ: expected "
                f"{sorted(expected_children)}, got {sorted(actual_children)}"
            )
        return {
            "source": source,
            "target": target,
            "node_id": after.node_id,
            "child_count": len(after.children),
            "children": sorted(after.children),
            "status": "already_moved",
        }

    before = inspect_subtree(client, source)
    result: dict[str, Any] = {
        "source": source,
        "target": target,
        "node_id": before.node_id,
        "child_count": len(before.children),
        "children": sorted(before.children),
        "status": "planned" if dry_run else "moved",
    }
    if dry_run:
        return result

    parent = _parent_path(target)
    if not client.exists(parent):
        client.create("map_node", parent, recursive=True)
    # Never pass force=True: a concurrently-created target must make this fail.
    client.move(source, target)

    if client.exists(source):
        raise RuntimeError(f"Source still exists after move: {source}")
    after = inspect_subtree(client, target)
    if after != before:
        raise RuntimeError(
            "Moved subtree inventory differs from the source inventory: "
            f"before={before!r}, after={after!r}"
        )
    return result


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--target", default=DEFAULT_TARGET)
    parser.add_argument("--proxy", default=os.environ.get("YT_PROXY", ""))
    parser.add_argument("--token", default=os.environ.get("YT_TOKEN") or None)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    if not args.proxy:
        raise SystemExit("YT proxy is required via --proxy or YT_PROXY")
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
    result = move_subtree(
        client,
        args.source,
        args.target,
        dry_run=args.dry_run,
        expected_children=EXPECTED_SHIKIMORI_CHILDREN,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
