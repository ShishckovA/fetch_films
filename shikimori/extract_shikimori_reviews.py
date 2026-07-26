#!/usr/bin/env python3
"""Extract saved Shikimori review pages into deterministic gzip JSONL.

The source directory contains a mixture of JSON fragments with reviews, full
HTML terminal pages, API error objects, age-restriction sentinels, and transient
error fragments.  Only ``article.b-review-topic`` nodes become output rows;
all other source shapes are classified in the manifest.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import os
import re
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from itertools import repeat
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urljoin, urlsplit, urlunsplit


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = PROJECT_DIR / "reviews"
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "reviews_parsed"

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
OPTIONAL_FIELDS = {"user_score", "user_list_status", "usefulness_ratio"}
RANKING_METHOD = "usefulness_score DESC, votes_for DESC, review_id DESC"
SCHEMA_VERSION = 1

DIRECTORY_RE = re.compile(r"^(?P<prefix>[a-z]?)(?P<id>\d+)-(?P<slug>.*)$", re.I)
REVIEW_PATH_RE = re.compile(
    r"^/animes/(?P<slug>[^/]+)/reviews/(?P<review_id>\d+)/?$"
)
SCORE_RE = re.compile(r"^score-(\d+)$")
INTEGER_RE = re.compile(r"-?\d+")
TRANSIENT_MESSAGE = (
    "Отзывы временно недоступны, на сайте проводятся технические работы."
)
VOID_TAGS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}
BLOCK_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "div",
    "dl",
    "dt",
    "dd",
    "figcaption",
    "figure",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tr",
    "ul",
}
USER_LIST_STATUSES = {
    "planned",
    "watching",
    "rewatching",
    "completed",
    "on_hold",
    "dropped",
}
OPINIONS = {"positive", "neutral", "negative"}


class ReviewParseError(ValueError):
    """A review article exists but violates the expected record contract."""


@dataclass
class Node:
    tag: str
    attrs: dict[str, str]
    children: list[Node | str] = field(default_factory=list)
    parent: Node | None = None

    @property
    def classes(self) -> set[str]:
        return set(self.attrs.get("class", "").split())


class FragmentParser(HTMLParser):
    """Small repair-tolerant DOM used to avoid another parser dependency."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = Node("[document]", {})
        self.stack = [self.root]

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        node = Node(
            tag.lower(),
            {name.lower(): value or "" for name, value in attrs},
            parent=self.stack[-1],
        )
        self.stack[-1].children.append(node)
        if node.tag not in VOID_TAGS:
            self.stack.append(node)

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        self.handle_starttag(tag, attrs)
        if self.stack[-1].tag == tag.lower() and tag.lower() not in VOID_TAGS:
            self.stack.pop()

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                return

    def handle_data(self, data: str) -> None:
        self.stack[-1].children.append(data)


@dataclass(frozen=True)
class AnimeInput:
    anime_id: int
    directory_name: str
    pages: tuple[Path, ...]
    missing_pages: tuple[int, ...]


@dataclass
class AnimeResult:
    anime_id: int
    rows: list[dict[str, Any]]
    occurrence_count: int
    duplicate_count: int
    conflicting_duplicate_count: int
    statuses: Counter[str]
    file_digests: list[tuple[str, str, int]]
    declared_review_count: int | None
    declared_count_mismatch: bool
    terminal_postloader_pages: int
    warnings: list[str]


def sanitize(value: str) -> str:
    return value.replace("\x00", " ")


def parse_fragment(value: str) -> Node:
    parser = FragmentParser()
    parser.feed(value)
    parser.close()
    return parser.root


def iter_nodes(node: Node) -> Iterator[Node]:
    for child in node.children:
        if isinstance(child, Node):
            yield child
            yield from iter_nodes(child)


def find_first(
    node: Node,
    *,
    tag: str | None = None,
    class_name: str | None = None,
    attr: tuple[str, str] | None = None,
) -> Node | None:
    for candidate in iter_nodes(node):
        if tag is not None and candidate.tag != tag:
            continue
        if class_name is not None and class_name not in candidate.classes:
            continue
        if attr is not None and candidate.attrs.get(attr[0]) != attr[1]:
            continue
        return candidate
    return None


def find_all(
    node: Node,
    *,
    tag: str | None = None,
    class_name: str | None = None,
) -> list[Node]:
    result = []
    for candidate in iter_nodes(node):
        if tag is not None and candidate.tag != tag:
            continue
        if class_name is not None and class_name not in candidate.classes:
            continue
        result.append(candidate)
    return result


def direct_meta(article: Node, itemprop: str) -> str:
    for child in article.children:
        if (
            isinstance(child, Node)
            and child.tag == "meta"
            and child.attrs.get("itemprop") == itemprop
        ):
            return sanitize(child.attrs.get("content", ""))
    return ""


def simple_text(node: Node, *, skip_markers: bool = False) -> str:
    parts: list[str] = []

    def visit(value: Node | str) -> None:
        if isinstance(value, str):
            parts.append(value)
            return
        if value.tag in {"script", "style"}:
            return
        if skip_markers and "marker" in value.classes:
            return
        if value.tag == "br":
            parts.append("\n")
            return
        if value.tag == "img" and value.attrs.get("alt"):
            parts.append(" " + value.attrs["alt"] + " ")
        is_block = value.tag in BLOCK_TAGS
        if is_block:
            parts.append("\n")
        for child in value.children:
            visit(child)
        if is_block:
            parts.append("\n")

    visit(node)
    lines = []
    for line in "".join(parts).replace("\xa0", " ").splitlines():
        normalized = re.sub(r"\s+", " ", sanitize(line)).strip()
        if normalized:
            lines.append(normalized)
    return "\n".join(lines)


def serialize_node(node: Node) -> str:
    attrs = "".join(
        f' {name}="{html.escape(sanitize(value), quote=True)}"'
        for name, value in node.attrs.items()
    )
    opening = f"<{node.tag}{attrs}>"
    if node.tag in VOID_TAGS:
        return opening
    contents = "".join(
        serialize_node(child)
        if isinstance(child, Node)
        else html.escape(sanitize(child), quote=False)
        for child in node.children
    )
    return f"{opening}{contents}</{node.tag}>"


def serialize_children(node: Node) -> str:
    return "".join(
        serialize_node(child)
        if isinstance(child, Node)
        else html.escape(sanitize(child), quote=False)
        for child in node.children
    )


def parse_integer(node: Node | None, field_name: str, source: str) -> int:
    if node is None:
        raise ReviewParseError(f"{source}: missing {field_name}")
    match = INTEGER_RE.search(re.sub(r"\s+", "", simple_text(node)))
    if not match:
        raise ReviewParseError(f"{source}: invalid {field_name}")
    return int(match.group())


def normalized_url(base: str, raw_url: str) -> str:
    return sanitize(urljoin(base, html.unescape(raw_url))) if raw_url else ""


def extract_body_links(body: Node, review_url: str) -> list[dict[str, Any]]:
    links = []
    for node in find_all(body, tag="a"):
        href = sanitize(node.attrs.get("href", ""))
        if not href:
            continue
        links.append(
            {
                "href": href,
                "url": normalized_url(review_url, href),
                "text": simple_text(node, skip_markers=True),
                "title": sanitize(node.attrs.get("title", "")),
                "rel": sanitize(node.attrs.get("rel", "")),
            }
        )
    return links


def extract_body_images(body: Node, review_url: str) -> list[dict[str, Any]]:
    images = []
    for node in find_all(body, tag="img"):
        src = sanitize(node.attrs.get("src", ""))
        images.append(
            {
                "src": src,
                "url": normalized_url(review_url, src),
                "srcset": sanitize(node.attrs.get("srcset", "")),
                "alt": sanitize(node.attrs.get("alt", "")),
                "title": sanitize(node.attrs.get("title", "")),
                "width": sanitize(node.attrs.get("width", "")),
                "height": sanitize(node.attrs.get("height", "")),
                "data_width": sanitize(node.attrs.get("data-width", "")),
                "data_height": sanitize(node.attrs.get("data-height", "")),
                "classes": sorted(node.classes),
            }
        )
    return images


def required_attr(node: Node, name: str, source: str) -> str:
    value = sanitize(node.attrs.get(name, ""))
    if not value:
        raise ReviewParseError(f"{source}: missing {name}")
    return value


def extract_review(
    article: Node,
    *,
    expected_anime_id: int,
    source_file: str,
    source_page: int,
    source_position: int,
) -> dict[str, Any]:
    source = f"{source_file}: review #{source_position}"
    review_url = required_attr(article, "data-url", source)
    parsed_url = urlsplit(review_url)
    path_match = REVIEW_PATH_RE.match(parsed_url.path)
    if not path_match:
        raise ReviewParseError(f"{source}: invalid review URL {review_url!r}")
    anime_slug = path_match.group("slug")
    slug_match = re.match(r"^[a-z]?(\d+)-", anime_slug, re.I)
    if not slug_match or int(slug_match.group(1)) != expected_anime_id:
        raise ReviewParseError(f"{source}: anime id differs from directory")
    review_id = int(path_match.group("review_id"))

    topic_id = int(required_attr(article, "data-track_topic", source))
    article_id = required_attr(article, "id", source)
    if article_id != str(topic_id):
        raise ReviewParseError(f"{source}: topic ids differ")

    meta_url = direct_meta(article, "url")
    if meta_url and meta_url != review_url:
        raise ReviewParseError(f"{source}: review URLs differ")
    anime_title = direct_meta(article, "name") or direct_meta(article, "headline")
    if not anime_title:
        raise ReviewParseError(f"{source}: missing anime title")

    author_id = int(required_attr(article, "data-user_id", source))
    author_snapshot = required_attr(article, "data-user_nickname", source)
    meta_author = direct_meta(article, "author")
    if meta_author and meta_author != author_snapshot:
        raise ReviewParseError(f"{source}: author snapshots differ")

    details = find_first(article, class_name="review-details")
    author_name_node = (
        find_first(details, tag="a", class_name="name") if details else None
    )
    if author_name_node is None:
        raise ReviewParseError(f"{source}: missing visible author name")
    author_nickname = simple_text(author_name_node)
    if not author_nickname:
        raise ReviewParseError(f"{source}: empty visible author name")
    author_url = normalized_url(review_url, author_name_node.attrs.get("href", ""))

    author_link = find_first(article, tag="a", class_name="author-link")
    avatar = find_first(author_link, tag="img") if author_link else None
    author_avatar_url = normalized_url(
        review_url, avatar.attrs.get("src", "") if avatar else ""
    )
    author_avatar_srcset = sanitize(
        avatar.attrs.get("srcset", "") if avatar else ""
    )

    review_info = find_first(article, class_name="review-info")
    if review_info is None:
        raise ReviewParseError(f"{source}: missing review info")
    opinion_node = find_first(review_info, class_name="opinion")
    opinion_values = opinion_node.classes & OPINIONS if opinion_node else set()
    if len(opinion_values) != 1:
        raise ReviewParseError(f"{source}: invalid opinion")
    opinion = next(iter(opinion_values))

    votes_for = parse_integer(
        find_first(article, class_name="votes-for"), "votes_for", source
    )
    votes_against = parse_integer(
        find_first(article, class_name="votes-against"), "votes_against", source
    )
    comments_count = parse_integer(
        find_first(article, class_name="comments"), "comments_count", source
    )
    if min(votes_for, votes_against, comments_count) < 0:
        raise ReviewParseError(f"{source}: negative count")

    user_score = None
    for node in find_all(review_info, class_name="score"):
        for class_name in node.classes:
            match = SCORE_RE.match(class_name)
            if match:
                user_score = int(match.group(1))
                break
        if user_score is not None:
            break
    user_list_status = None
    list_node = find_first(review_info, class_name="b-add_to_list")
    if list_node:
        values = list_node.classes & USER_LIST_STATUSES
        if len(values) == 1:
            user_list_status = next(iter(values))
        elif values:
            raise ReviewParseError(f"{source}: ambiguous user list status")

    created_at = direct_meta(article, "dateCreated")
    published_at = direct_meta(article, "datePublished")
    updated_at = direct_meta(article, "dateModified")
    if not created_at or not published_at or not updated_at:
        raise ReviewParseError(f"{source}: missing timestamp")

    body = find_first(article, tag="div", attr=("itemprop", "text"))
    if body is None or "body" not in body.classes:
        raise ReviewParseError(f"{source}: missing review body")
    body_html = serialize_children(body)
    body_text = simple_text(body, skip_markers=True)

    anime_path = f"/animes/{anime_slug}"
    anime_url = urlunsplit(
        (parsed_url.scheme, parsed_url.netloc, anime_path, "", "")
    )
    votes_total = votes_for + votes_against
    usefulness_score = votes_for - votes_against
    result: dict[str, Any] = {
        "anime_id": expected_anime_id,
        "review_id": review_id,
        "topic_id": topic_id,
        "anime_slug": anime_slug,
        "anime_title": anime_title,
        "anime_url": anime_url,
        "review_url": review_url,
        "author_id": author_id,
        "author_nickname": author_nickname,
        "author_nickname_snapshot": author_snapshot,
        "author_url": author_url,
        "author_avatar_url": author_avatar_url,
        "author_avatar_srcset": author_avatar_srcset,
        "opinion": opinion,
        "votes_for": votes_for,
        "votes_against": votes_against,
        "votes_total": votes_total,
        "usefulness_score": usefulness_score,
        "comments_count": comments_count,
        "created_at": created_at,
        "published_at": published_at,
        "updated_at": updated_at,
        "is_written_before_release": any(
            "is_written_before_release" in node.classes
            and all(ancestor is not body for ancestor in ancestors(node))
            for node in iter_nodes(article)
        ),
        "body_text": body_text,
        "body_html": body_html,
        "body_links": extract_body_links(body, review_url),
        "body_images": extract_body_images(body, review_url),
        "inline_spoilers_count": sum(
            "b-spoiler_inline" in node.classes for node in iter_nodes(body)
        ),
        "block_spoilers_count": sum(
            bool(node.classes & {"b-spoiler", "b-spoiler_block"})
            for node in iter_nodes(body)
        ),
        "source_file": source_file,
        "source_page": source_page,
        "source_position": source_position,
    }
    if user_score is not None:
        result["user_score"] = user_score
    if user_list_status is not None:
        result["user_list_status"] = user_list_status
    if votes_total:
        result["usefulness_ratio"] = votes_for / votes_total
    return result


def review_articles(root: Node) -> list[Node]:
    articles = []
    for node in find_all(root, tag="article", class_name="b-review-topic"):
        if any(
            ancestor.tag == "article" and "b-review-topic" in ancestor.classes
            for ancestor in ancestors(node)
        ):
            continue
        articles.append(node)
    return articles


def ancestors(node: Node) -> Iterator[Node]:
    parent = node.parent
    while parent is not None:
        yield parent
        parent = parent.parent


def declared_review_count(root: Node) -> int | None:
    for node in find_all(root, class_name="navigation-node-all"):
        count = find_first(node, class_name="count")
        if count:
            match = INTEGER_RE.search(simple_text(count).replace(" ", ""))
            if match:
                return int(match.group())
    return None


def is_matching_anime_show(root: Node, expected_anime_id: int) -> bool:
    body = find_first(root, tag="body")
    if body is None or not {"p-animes", "p-animes-show"}.issubset(body.classes):
        return False
    for node in find_all(root, tag="meta"):
        if node.attrs.get("itemprop") != "url":
            continue
        path = urlsplit(node.attrs.get("content", "")).path
        match = re.match(r"^/animes/[a-z]?(\d+)(?:-|$)", path, re.I)
        if match and int(match.group(1)) == expected_anime_id:
            return True
    return False


def classify_and_extract_page(
    raw: bytes,
    *,
    expected_anime_id: int,
    source_file: str,
    source_page: int,
) -> tuple[str, list[dict[str, Any]], int | None, bool]:
    text = raw.decode("utf-8", errors="replace")
    stripped = text.strip()
    if stripped == "age_restricted":
        return "age_restricted", [], None, False

    if stripped.startswith("<"):
        root = parse_fragment(text)
        articles = review_articles(root)
        rows = [
            extract_review(
                article,
                expected_anime_id=expected_anime_id,
                source_file=source_file,
                source_page=source_page,
                source_position=position,
            )
            for position, article in enumerate(articles, 1)
        ]
        return (
            (
                "html_with_reviews"
                if rows
                else (
                    "html_terminal"
                    if is_matching_anime_show(root, expected_anime_id)
                    else "html_unknown"
                )
            ),
            rows,
            declared_review_count(root),
            False,
        )

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return "malformed_json", [], None, False
    if not isinstance(payload, dict):
        return "json_non_object", [], None, False
    content = payload.get("content")
    if not isinstance(content, str):
        status = payload.get("status")
        return f"api_error_{status}" if status is not None else "api_error", [], None, False

    root = parse_fragment(content)
    articles = review_articles(root)
    if not articles:
        content_text = simple_text(root)
        if TRANSIENT_MESSAGE in content_text:
            return "transient_unavailable", [], None, "postloader" in payload
        return "json_no_reviews", [], None, "postloader" in payload
    rows = [
        extract_review(
            article,
            expected_anime_id=expected_anime_id,
            source_file=source_file,
            source_page=source_page,
            source_position=position,
        )
        for position, article in enumerate(articles, 1)
    ]
    return "reviews_json", rows, None, "postloader" in payload


def rows_equal_ignoring_source(
    left: dict[str, Any], right: dict[str, Any]
) -> bool:
    ignored = {"source_file", "source_page", "source_position"}
    return {k: v for k, v in left.items() if k not in ignored} == {
        k: v for k, v in right.items() if k not in ignored
    }


def parse_anime(input_data: AnimeInput, input_root: Path) -> AnimeResult:
    statuses: Counter[str] = Counter()
    file_digests = []
    occurrences: list[dict[str, Any]] = []
    declared_count = None
    warnings = []
    if not input_data.pages:
        statuses["empty_directory"] += 1
    if input_data.missing_pages:
        statuses["missing_page"] += len(input_data.missing_pages)
        warnings.append(
            f"{input_data.directory_name}: missing pages {input_data.missing_pages}"
        )

    for path in input_data.pages:
        raw = path.read_bytes()
        relative = path.relative_to(input_root).as_posix()
        file_digests.append((relative, hashlib.sha256(raw).hexdigest(), len(raw)))
        page = int(path.stem)
        status, rows, page_declared_count, has_postloader = classify_and_extract_page(
            raw,
            expected_anime_id=input_data.anime_id,
            source_file=relative,
            source_page=page,
        )
        statuses[status] += 1
        occurrences.extend(rows)
        if (
            status
            in {
                "transient_unavailable",
                "age_restricted",
                "malformed_json",
                "json_non_object",
                "json_no_reviews",
                "html_unknown",
            }
            or (status.startswith("api_error") and status != "api_error_404")
        ):
            warnings.append(f"{relative}: {status}")
        if page_declared_count is not None:
            declared_count = page_declared_count
        if has_postloader and path == input_data.pages[-1]:
            statuses["last_page_has_postloader"] += 1
            warnings.append(
                f"{relative}: last_page_has_postloader "
                "(may be exact page-size multiple)"
            )

    by_id: dict[int, dict[str, Any]] = {}
    duplicate_count = 0
    conflicting_count = 0
    for row in occurrences:
        review_id = row["review_id"]
        previous = by_id.get(review_id)
        if previous is None:
            by_id[review_id] = row
            continue
        duplicate_count += 1
        if not rows_equal_ignoring_source(previous, row):
            conflicting_count += 1
        if (row["source_page"], row["source_position"]) > (
            previous["source_page"],
            previous["source_position"],
        ):
            by_id[review_id] = row

    rows = sorted(
        by_id.values(),
        key=lambda row: (
            -row["usefulness_score"],
            -row["votes_for"],
            -row["review_id"],
        ),
    )
    for rank, row in enumerate(rows, 1):
        row["usefulness_rank"] = rank
        ordered = {field: row[field] for field in FIELDS if field in row}
        actual = set(ordered)
        missing = set(FIELDS) - OPTIONAL_FIELDS - actual
        extra = set(row) - set(FIELDS)
        if missing or extra:
            raise ReviewParseError(
                f"{row['source_file']}: output fields mismatch; "
                f"missing={sorted(missing)}, extra={sorted(extra)}"
            )
        rows[rank - 1] = ordered

    mismatch = declared_count is not None and declared_count != len(rows)
    if mismatch:
        warnings.append(
            f"{input_data.directory_name}: declared {declared_count} reviews, "
            f"extracted {len(rows)}"
        )
    return AnimeResult(
        anime_id=input_data.anime_id,
        rows=rows,
        occurrence_count=len(occurrences),
        duplicate_count=duplicate_count,
        conflicting_duplicate_count=conflicting_count,
        statuses=statuses,
        file_digests=file_digests,
        declared_review_count=declared_count,
        declared_count_mismatch=mismatch,
        terminal_postloader_pages=statuses["last_page_has_postloader"],
        warnings=warnings,
    )


def discover_inputs(input_dir: Path) -> tuple[list[AnimeInput], str, int, int]:
    directories = []
    seen_ids: dict[int, str] = {}
    inventory = hashlib.sha256()
    source_file_count = 0
    source_bytes = 0
    for directory in input_dir.iterdir():
        if not directory.is_dir():
            continue
        match = DIRECTORY_RE.match(directory.name)
        if not match:
            raise ValueError(f"Unexpected review directory: {directory}")
        anime_id = int(match.group("id"))
        if anime_id in seen_ids:
            raise ValueError(
                f"Duplicate anime id {anime_id}: {seen_ids[anime_id]}, {directory.name}"
            )
        seen_ids[anime_id] = directory.name
        pages = []
        for path in directory.iterdir():
            if path.name.startswith("."):
                continue
            if not path.is_file() or path.suffix != ".json" or not path.stem.isdigit():
                raise ValueError(f"Unexpected review source file: {path}")
            pages.append(path)
        pages.sort(key=lambda path: int(path.stem))
        page_numbers = [int(path.stem) for path in pages]
        missing = (
            tuple(sorted(set(range(1, max(page_numbers) + 1)) - set(page_numbers)))
            if page_numbers
            else ()
        )
        directories.append(
            AnimeInput(anime_id, directory.name, tuple(pages), missing)
        )

    directories.sort(key=lambda item: (item.anime_id, item.directory_name))
    for item in directories:
        relative_dir = item.directory_name.encode("utf-8")
        inventory.update(relative_dir + b"\0D\0")
        for path in item.pages:
            stat = path.stat()
            relative = path.relative_to(input_dir).as_posix().encode("utf-8")
            inventory.update(relative + b"\0")
            inventory.update(str(stat.st_size).encode("ascii") + b"\0")
            inventory.update(str(stat.st_mtime_ns).encode("ascii") + b"\0")
            source_file_count += 1
            source_bytes += stat.st_size
    return directories, inventory.hexdigest(), source_file_count, source_bytes


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def cached_manifest(
    output_path: Path,
    manifest_path: Path,
    inventory_sha256: str,
    extractor_sha256: str,
) -> dict[str, Any] | None:
    if not output_path.is_file() or not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict):
        return None
    if (
        manifest.get("entity") != "reviews"
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("fields") != list(FIELDS)
        or manifest.get("ranking_method") != RANKING_METHOD
        or manifest.get("extractor_sha256") != extractor_sha256
        or manifest.get("complete") is not True
        or manifest.get("extraction_complete") is not True
        or manifest.get("source_inventory_sha256") != inventory_sha256
        or manifest.get("jsonl_size_bytes") != output_path.stat().st_size
        or manifest.get("jsonl_sha256") != file_sha256(output_path)
    ):
        return None
    return manifest


def source_tree_update(digest: Any, relative: str, file_digest: str) -> None:
    digest.update(relative.encode("utf-8"))
    digest.update(b"\0")
    digest.update(bytes.fromhex(file_digest))


def iter_results(
    inputs: list[AnimeInput], input_dir: Path, workers: int
) -> Iterable[AnimeResult]:
    if workers == 1:
        return (parse_anime(item, input_dir) for item in inputs)
    executor = ProcessPoolExecutor(max_workers=workers)
    results = executor.map(parse_anime, inputs, repeat(input_dir), chunksize=1)

    def close_executor() -> Iterator[AnimeResult]:
        try:
            yield from results
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

    return close_executor()


def extract_reviews(
    input_dir: Path,
    output_dir: Path,
    *,
    workers: int = 1,
    force: bool = False,
    progress_every: int = 1000,
) -> dict[str, Any]:
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    if not input_dir.is_dir():
        raise ValueError(f"Input directory does not exist: {input_dir}")
    if workers <= 0 or progress_every < 0:
        raise ValueError("workers must be positive and progress_every non-negative")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "reviews.jsonl.gz"
    manifest_path = output_dir / "manifest.json"
    output_part = output_dir / "reviews.jsonl.gz.part"
    manifest_part = output_dir / "manifest.json.part"

    inputs, inventory_digest, source_file_count, source_bytes = discover_inputs(
        input_dir
    )
    extractor_digest = file_sha256(Path(__file__).resolve())
    if not force:
        cached = cached_manifest(
            output_path, manifest_path, inventory_digest, extractor_digest
        )
        if cached is not None:
            result = dict(cached)
            result["status"] = "cached"
            return result

    output_part.unlink(missing_ok=True)
    manifest_part.unlink(missing_ok=True)
    statuses: Counter[str] = Counter()
    source_tree = hashlib.sha256()
    warnings: list[str] = []
    record_count = 0
    occurrence_count = 0
    anime_count = 0
    top_50_count = 0
    duplicate_count = 0
    conflict_count = 0
    mismatch_count = 0
    terminal_postloader_pages = 0
    seen_review_ids: set[int] = set()

    try:
        with output_part.open("wb") as raw_output:
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=raw_output, mtime=0
            ) as compressed:
                for index, result in enumerate(
                    iter_results(inputs, input_dir, workers), 1
                ):
                    statuses.update(result.statuses)
                    occurrence_count += result.occurrence_count
                    duplicate_count += result.duplicate_count
                    conflict_count += result.conflicting_duplicate_count
                    mismatch_count += int(result.declared_count_mismatch)
                    terminal_postloader_pages += result.terminal_postloader_pages
                    warnings.extend(result.warnings)
                    for relative, digest, _size in result.file_digests:
                        source_tree_update(source_tree, relative, digest)
                    if result.rows:
                        anime_count += 1
                        top_50_count += min(50, len(result.rows))
                    for row in result.rows:
                        review_id = row["review_id"]
                        if review_id in seen_review_ids:
                            raise ReviewParseError(
                                f"review id {review_id} occurs in multiple anime"
                            )
                        seen_review_ids.add(review_id)
                        compressed.write(
                            json.dumps(
                                row,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ).encode("utf-8")
                            + b"\n"
                        )
                        record_count += 1
                    if progress_every and index % progress_every == 0:
                        print(
                            f"reviews: processed {index}/{len(inputs)} anime dirs, "
                            f"rows={record_count}",
                            file=sys.stderr,
                            flush=True,
                        )
            raw_output.flush()
            os.fsync(raw_output.fileno())

        incomplete_statuses = {
            "transient_unavailable",
            "age_restricted",
            "malformed_json",
            "json_non_object",
            "json_no_reviews",
            "html_unknown",
            "missing_page",
        }
        incomplete_statuses.update(
            status
            for status in statuses
            if status.startswith("api_error") and status != "api_error_404"
        )
        incomplete_reasons = [
            f"{status}: {statuses[status]}"
            for status in sorted(incomplete_statuses)
            if statuses[status]
        ]
        if mismatch_count:
            incomplete_reasons.append(
                f"declared_review_count_mismatch_anime: {mismatch_count}"
            )
        source_pagination_complete = not incomplete_reasons
        summary_warnings = list(incomplete_reasons)
        if warnings:
            summary_warnings.append(
                f"{len(warnings)} per-anime warnings; inspect count diagnostics"
            )

        output_digest = file_sha256(output_part)
        manifest: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "extractor_sha256": extractor_digest,
            "entity": "reviews",
            "complete": True,
            "extraction_complete": True,
            "source_pagination_complete": source_pagination_complete,
            "source_incomplete_reasons": incomplete_reasons,
            "warnings": summary_warnings,
            "source_issue_details": warnings,
            "source_dir": str(input_dir),
            "source_directory_count": len(inputs),
            "source_file_count": source_file_count,
            "source_bytes": source_bytes,
            "source_inventory_sha256": inventory_digest,
            "source_inventory_sha256_algorithm": (
                "sha256(path,NUL,size,NUL,mtime_ns,NUL; directories included)"
            ),
            "source_tree_sha256": source_tree.hexdigest(),
            "source_tree_sha256_algorithm": (
                "sha256(sorted(relative_path,NUL,sha256(file_bytes)))"
            ),
            "source_status_counts": dict(sorted(statuses.items())),
            "terminal_postloader_pages": terminal_postloader_pages,
            "declared_review_count_mismatch_anime_count": mismatch_count,
            "record_occurrence_count": occurrence_count,
            "record_count": record_count,
            "anime_count": anime_count,
            "top_50_record_count": top_50_count,
            "duplicate_count": duplicate_count,
            "conflicting_duplicate_count": conflict_count,
            "ranking_method": RANKING_METHOD,
            "user_score_semantics": (
                "raw CSS score-N; 0 means a user rate exists without a numeric "
                "score; absent means no user rate block"
            ),
            "fields": list(FIELDS),
            "jsonl": str(output_path),
            "jsonl_sha256": output_digest,
            "jsonl_size_bytes": output_part.stat().st_size,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_part.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(output_part, output_path)
        os.replace(manifest_part, manifest_path)
        result = dict(manifest)
        result["status"] = "extracted"
        return result
    except BaseException:
        output_part.unlink(missing_ok=True)
        manifest_part.unlink(missing_ok=True)
        raise


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--progress-every", type=int, default=1000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    try:
        result = extract_reviews(
            args.input_dir,
            args.output_dir,
            workers=args.workers,
            force=args.force,
            progress_every=args.progress_every,
        )
    except (OSError, ValueError, ReviewParseError) as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
