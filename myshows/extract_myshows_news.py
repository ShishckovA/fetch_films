#!/usr/bin/env python3
"""Extract saved MyShows news pages into deterministic gzip JSONL.

Every numeric ``*.html`` input becomes exactly one output row.  Normal article
pages are decoded from Nuxt's flattened devalue payload; expected 404 pages and
unexpected parser failures are retained as diagnostic rows instead of being
dropped.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import math
import os
import re
import sys
from collections import Counter, deque
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urljoin, urlsplit


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = PROJECT_DIR / "news"
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "news_parsed"
BASE_URL = "https://myshows.me/"
SCHEMA_VERSION = 1

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

STATUSES = {"ok", "not_found", "nuxt_error", "missing_news", "parse_error"}
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
DICT_FIELDS = {
    "comments_meta",
    "author",
    "nuxt_news",
    "nuxt_errors",
}

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
    "main",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tr",
    "ul",
}

NEWS_URL_RE = re.compile(r"^/news/(?P<id>\d+)(?:/(?P<slug>[^/]+))?/?$")
INTEGER_RE = re.compile(r"-?\d+")
AUTHOR_ARTICLES_RE = re.compile(r"([\d\s]+)\s+стат", re.IGNORECASE)


@dataclass
class Node:
    tag: str
    attrs: dict[str, str]
    children: list[Node | str] = field(default_factory=list)
    parent: Node | None = None

    @property
    def classes(self) -> set[str]:
        return set(self.attrs.get("class", "").split())


class DocumentParser(HTMLParser):
    """A small, repair-tolerant DOM with no third-party dependency."""

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


class NuxtDecodeError(ValueError):
    """The ``__NUXT_DATA__`` payload is absent or malformed."""


class DevalueDecoder:
    """Decode the flattened devalue representation embedded by Nuxt.

    Only selected root values are hydrated by the extractor.  This avoids
    expanding unrelated Pinia state for pages with hundreds of comments.
    """

    SPECIAL_REFERENCES = {
        -1: None,  # undefined
        -2: None,  # array hole
        -3: "NaN",
        -4: "Infinity",
        -5: "-Infinity",
        -6: -0.0,
    }
    WRAPPERS = {"ShallowReactive", "Reactive", "ShallowRef", "Ref", "EmptyRef"}

    def __init__(self, raw: str) -> None:
        try:
            values = json.loads(raw)
        except json.JSONDecodeError as error:
            raise NuxtDecodeError(f"invalid __NUXT_DATA__ JSON: {error}") from error
        if not isinstance(values, list) or not values:
            raise NuxtDecodeError("__NUXT_DATA__ is not a non-empty flattened list")
        self.values = values
        self.cache: dict[int, Any] = {}
        self.active: set[int] = set()

    def _reference(self, value: Any) -> Any:
        if isinstance(value, int) and not isinstance(value, bool):
            return self.hydrate(value)
        return value

    def hydrate(self, index: int) -> Any:
        if index < 0:
            if index not in self.SPECIAL_REFERENCES:
                raise NuxtDecodeError(f"unknown negative devalue reference: {index}")
            return self.SPECIAL_REFERENCES[index]
        if index >= len(self.values):
            raise NuxtDecodeError(f"devalue reference out of range: {index}")
        if index in self.active:
            return "<cycle>"
        if index in self.cache:
            return self.cache[index]

        self.active.add(index)
        try:
            value = self.values[index]
            if isinstance(value, dict):
                result: Any = {}
                self.cache[index] = result
                for key, child in value.items():
                    result[key] = self._reference(child)
            elif isinstance(value, list):
                result = self._hydrate_list(index, value)
                self.cache[index] = result
            else:
                result = value
                self.cache[index] = result
            return result
        finally:
            self.active.remove(index)

    def _hydrate_list(self, index: int, value: list[Any]) -> Any:
        if value and isinstance(value[0], str):
            tag = value[0]
            if tag in self.WRAPPERS:
                return self._reference(value[1]) if len(value) > 1 else None
            if tag == "null":
                result: dict[str, Any] = {}
                self.cache[index] = result
                if (len(value) - 1) % 2:
                    raise NuxtDecodeError("invalid null-prototype object")
                for position in range(1, len(value), 2):
                    key = value[position]
                    if not isinstance(key, str):
                        raise NuxtDecodeError("invalid null-prototype object key")
                    result[key] = self._reference(value[position + 1])
                return result
            if tag == "Set":
                return [self._reference(child) for child in value[1:]]
            if tag == "Map":
                if (len(value) - 1) % 2:
                    raise NuxtDecodeError("invalid Map payload")
                pairs = []
                for position in range(1, len(value), 2):
                    pairs.append(
                        [
                            self._reference(value[position]),
                            self._reference(value[position + 1]),
                        ]
                    )
                return pairs
            if tag == "Date":
                return value[1] if len(value) > 1 else ""
            if tag == "BigInt":
                return int(value[1])
            if tag == "Object":
                return value[1] if len(value) > 1 else None
            if tag == "RegExp":
                return {
                    "source": value[1] if len(value) > 1 else "",
                    "flags": value[2] if len(value) > 2 else "",
                }
            if len(value) == 2:
                # Nuxt plugins can register custom reducers.  Preserve their
                # tag and decoded payload rather than silently discarding it.
                return {"__nuxt_type__": tag, "value": self._reference(value[1])}
        return [self._reference(child) for child in value]

    def root_references(self) -> dict[str, Any]:
        index = 0
        seen = set()
        while True:
            if index in seen:
                raise NuxtDecodeError("cyclic wrapper around Nuxt root")
            seen.add(index)
            raw = self.values[index]
            if (
                isinstance(raw, list)
                and raw
                and raw[0] in self.WRAPPERS
                and len(raw) > 1
                and isinstance(raw[1], int)
            ):
                index = raw[1]
                continue
            if not isinstance(raw, dict):
                raise NuxtDecodeError("Nuxt root does not resolve to an object")
            return raw

    def root_value(self, name: str, default: Any = None) -> Any:
        references = self.root_references()
        if name not in references:
            return default
        return self._reference(references[name])


@dataclass(frozen=True)
class SourceInput:
    news_id: int
    path: Path


@dataclass
class ParseResult:
    row: dict[str, Any]
    source_digest: str
    issue: str | None


def sanitize(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\x00", " ")


def parse_document(value: str) -> Node:
    parser = DocumentParser()
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
    node: Node, *, tag: str | None = None, class_name: str | None = None
) -> list[Node]:
    result = []
    for candidate in iter_nodes(node):
        if tag is not None and candidate.tag != tag:
            continue
        if class_name is not None and class_name not in candidate.classes:
            continue
        result.append(candidate)
    return result


def simple_text(node: Node | None) -> str:
    if node is None:
        return ""
    parts: list[str] = []

    def visit(value: Node | str) -> None:
        if isinstance(value, str):
            parts.append(value)
            return
        if value.tag in {"script", "style", "template"}:
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


def serialize_children(node: Node | None) -> str:
    if node is None:
        return ""
    return "".join(
        serialize_node(child)
        if isinstance(child, Node)
        else html.escape(sanitize(child), quote=False)
        for child in node.children
    )


def script_text(node: Node) -> str:
    return "".join(child for child in node.children if isinstance(child, str)).strip()


def normalized_url(raw_url: Any, base: str = BASE_URL) -> str:
    value = sanitize(raw_url).strip()
    return urljoin(base, html.unescape(value)) if value else ""


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def optional_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        match = INTEGER_RE.search(value.replace(" ", ""))
        return int(match.group()) if match else None
    return None


def json_safe(value: Any, active: set[int] | None = None) -> Any:
    """Return deterministic JSON-compatible data and break unexpected cycles."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if active is None:
        active = set()
    identity = id(value)
    if identity in active:
        return "<cycle>"
    active.add(identity)
    try:
        if isinstance(value, dict):
            return {
                sanitize(key): json_safe(child, active)
                for key, child in value.items()
            }
        if isinstance(value, (list, tuple, set)):
            return [json_safe(child, active) for child in value]
        return sanitize(value)
    finally:
        active.remove(identity)


def extract_meta(root: Node) -> tuple[list[dict[str, str]], dict[str, str]]:
    records: list[dict[str, str]] = []
    lookup: dict[str, str] = {}
    for node in find_all(root, tag="meta"):
        key_type = ""
        key = ""
        for candidate in ("property", "name", "itemprop", "http-equiv"):
            if node.attrs.get(candidate):
                key_type = candidate
                key = sanitize(node.attrs[candidate])
                break
        if not key:
            continue
        content = sanitize(node.attrs.get("content", ""))
        records.append({"key_type": key_type, "key": key, "content": content})
        lookup.setdefault(key.lower(), content)
    return records, lookup


def extract_links(root: Node) -> tuple[str, list[dict[str, str]]]:
    canonical = ""
    alternates = []
    for node in find_all(root, tag="link"):
        rel_tokens = set(node.attrs.get("rel", "").lower().split())
        href = normalized_url(node.attrs.get("href", ""))
        if "canonical" in rel_tokens and href and not canonical:
            canonical = href
        hreflang = sanitize(node.attrs.get("hreflang", ""))
        if "alternate" in rel_tokens and hreflang and href:
            alternates.append(
                {
                    "hreflang": hreflang,
                    "href": sanitize(node.attrs.get("href", "")),
                    "url": href,
                }
            )
    return canonical, alternates


def extract_json_ld(root: Node, warnings: list[str]) -> list[Any]:
    result = []
    for position, node in enumerate(find_all(root, tag="script"), 1):
        if node.attrs.get("type", "").lower() != "application/ld+json":
            continue
        raw = script_text(node)
        try:
            result.append(json.loads(raw))
        except json.JSONDecodeError as error:
            warnings.append(f"JSON-LD script {position} is invalid: {error}")
            result.append({"raw": raw, "parse_error": str(error)})
    return json_safe(result)


def first_json_ld_type(records: list[Any], expected: str) -> dict[str, Any]:
    for item in records:
        candidates = item if isinstance(item, list) else [item]
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("@type") == expected:
                return candidate
    return {}


def extract_content_assets(content_html: str, base_url: str) -> tuple[list[Any], list[Any], str]:
    if not content_html:
        return [], [], ""
    root = parse_document(content_html)
    links = []
    for node in find_all(root, tag="a"):
        href = sanitize(node.attrs.get("href", ""))
        if not href:
            continue
        links.append(
            {
                "href": href,
                "url": normalized_url(href, base_url),
                "text": simple_text(node),
                "title": sanitize(node.attrs.get("title", "")),
                "rel": sanitize(node.attrs.get("rel", "")),
                "target": sanitize(node.attrs.get("target", "")),
            }
        )
    images = []
    for node in find_all(root, tag="img"):
        src = sanitize(node.attrs.get("src", ""))
        images.append(
            {
                "src": src,
                "url": normalized_url(src, base_url),
                "srcset": sanitize(node.attrs.get("srcset", "")),
                "alt": sanitize(node.attrs.get("alt", "")),
                "title": sanitize(node.attrs.get("title", "")),
                "width": sanitize(node.attrs.get("width", "")),
                "height": sanitize(node.attrs.get("height", "")),
                "loading": sanitize(node.attrs.get("loading", "")),
            }
        )
    return links, images, simple_text(root)


def find_nuxt_script(root: Node) -> Node:
    for node in find_all(root, tag="script"):
        if node.attrs.get("id") == "__NUXT_DATA__":
            return node
    raise NuxtDecodeError("missing __NUXT_DATA__ script")


def select_page_data(
    data: Any, news_id: int, path_hint: str, warnings: list[str]
) -> tuple[str, dict[str, Any]]:
    if not isinstance(data, dict):
        warnings.append("Nuxt data is not an object")
        return "", {}
    candidates: list[tuple[str, dict[str, Any]]] = []
    for route, value in data.items():
        if isinstance(value, dict):
            candidates.append((sanitize(route), value))
    if path_hint and isinstance(data.get(path_hint), dict):
        return path_hint, data[path_hint]
    for route, value in candidates:
        news = as_dict(value.get("news"))
        if optional_int(news.get("id")) == news_id:
            return route, value
    for route, value in candidates:
        if isinstance(value.get("news"), dict):
            warnings.append(f"selected unmatched Nuxt news route {route!r}")
            return route, value
    if candidates:
        return candidates[0]
    return "", {}


def error_details(error: Any) -> tuple[int | None, str]:
    if not isinstance(error, dict):
        return None, sanitize(error) if error else ""
    status = optional_int(error.get("statusCode"))
    if status is None:
        status = optional_int(error.get("status"))
    parts = []
    for key in ("statusMessage", "message"):
        value = sanitize(error.get(key, "")).strip()
        if value and value not in parts:
            parts.append(value)
    return status, ": ".join(parts)


def empty_row(news_id: int, relative: str, size: int, digest: str) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for field_name in FIELDS:
        if field_name in LIST_FIELDS:
            row[field_name] = []
        elif field_name in DICT_FIELDS:
            row[field_name] = {}
        elif field_name in OPTIONAL_INTEGER_FIELDS:
            row[field_name] = None
        elif field_name in {"comments_complete", "has_spoilers"}:
            row[field_name] = False
        elif field_name in {
            "reaction_like_count",
            "reaction_fire_count",
            "reaction_dislike_count",
            "reaction_love_count",
            "reaction_anger_count",
            "reaction_shock_count",
            "reaction_total",
        }:
            row[field_name] = 0
        else:
            row[field_name] = ""
    row.update(
        {
            "source_id": news_id,
            "source_file": relative,
            "source_bytes": size,
            "source_sha256": digest,
        }
    )
    return row


def _first_dom_image(root: Node) -> Node | None:
    poster = find_first(root, class_name="NewsDetails__poster")
    return find_first(poster, tag="img") if poster else None


def _article_author(article_ld: dict[str, Any]) -> dict[str, Any]:
    value = article_ld.get("author")
    if isinstance(value, list):
        return next((item for item in value if isinstance(item, dict)), {})
    return as_dict(value)


def _main_entity_url(article_ld: dict[str, Any]) -> str:
    value = article_ld.get("mainEntityOfPage")
    if isinstance(value, str):
        return normalized_url(value)
    if isinstance(value, dict):
        return normalized_url(value.get("@id") or value.get("url"))
    return ""


def _reaction_counts(emotions: list[Any]) -> dict[str, int]:
    field_by_id = {
        2: "reaction_like_count",
        3: "reaction_fire_count",
        4: "reaction_dislike_count",
        5: "reaction_love_count",
        6: "reaction_anger_count",
        7: "reaction_shock_count",
    }
    result = {field_name: 0 for field_name in field_by_id.values()}
    for item in emotions:
        if not isinstance(item, dict):
            continue
        field_name = field_by_id.get(optional_int(item.get("emotionId")))
        count = optional_int(item.get("count"))
        if field_name is not None and count is not None:
            result[field_name] = count
    result["reaction_total"] = sum(result.values())
    return result


def loaded_comment_ids(comments: list[Any]) -> set[int]:
    """Collect every nested comment id from MyShows' threaded representation."""

    result: set[int] = set()
    visited: set[int] = set()

    def visit(value: Any) -> None:
        if isinstance(value, (dict, list)):
            identity = id(value)
            if identity in visited:
                return
            visited.add(identity)
        if isinstance(value, list):
            for child in value:
                visit(child)
            return
        if not isinstance(value, dict):
            return
        comment = value.get("comment")
        if isinstance(comment, dict):
            comment_id = optional_int(comment.get("id"))
            if comment_id is not None:
                result.add(comment_id)
        for key, child in value.items():
            if key != "comment" and isinstance(child, (dict, list)):
                visit(child)

    visit(comments)
    return result


def parse_source(source: SourceInput, input_dir: Path) -> ParseResult:
    raw_bytes = source.path.read_bytes()
    digest = hashlib.sha256(raw_bytes).hexdigest()
    relative = source.path.resolve().relative_to(input_dir.resolve()).as_posix()
    row = empty_row(source.news_id, relative, len(raw_bytes), digest)
    try:
        raw_text = raw_bytes.decode("utf-8", errors="replace")
        root = parse_document(raw_text)
        warnings: list[str] = []
        page_title = simple_text(find_first(root, tag="title"))
        meta_records, meta = extract_meta(root)
        canonical_url, hreflang = extract_links(root)
        json_ld = extract_json_ld(root, warnings)
        article_ld = first_json_ld_type(json_ld, "Article")

        nuxt_script = find_nuxt_script(root)
        decoder = DevalueDecoder(script_text(nuxt_script))
        nuxt_data = decoder.root_value("data", {})
        nuxt_error = decoder.root_value("error", None)
        nuxt_internal_errors = decoder.root_value("_errors", {})
        path_hint = sanitize(decoder.root_value("path", ""))
        route, page_data = select_page_data(
            nuxt_data, source.news_id, path_hint, warnings
        )
        news = as_dict(page_data.get("news"))

        error_code, error_message = error_details(nuxt_error)
        if error_code == 404:
            status = "not_found"
        elif nuxt_error:
            status = "nuxt_error"
        elif news:
            status = "ok"
        else:
            status = "missing_news"

        decoded_id = optional_int(news.get("id"))
        if decoded_id is not None and decoded_id != source.news_id:
            warnings.append(
                f"Nuxt news id {decoded_id} differs from filename id {source.news_id}"
            )

        canonical_url = canonical_url or normalized_url(meta.get("og:url"))
        canonical_url = canonical_url or _main_entity_url(article_ld)
        url = normalized_url(meta.get("og:url")) or canonical_url
        if not url:
            url = normalized_url(f"/news/{source.news_id}/")

        slug = sanitize(news.get("alias", ""))
        parsed_path = urlsplit(canonical_url or url).path
        match = NEWS_URL_RE.match(parsed_path)
        if match:
            if int(match.group("id")) != source.news_id:
                warnings.append(
                    f"canonical URL id {match.group('id')} differs from filename"
                )
            slug = slug or sanitize(match.group("slug") or "")

        dom_title = simple_text(find_first(root, class_name="NewsDetails__title"))
        dom_foreword = simple_text(
            find_first(root, class_name="NewsDetails__foreword")
        )
        content_html = sanitize(news.get("content", ""))
        if not content_html:
            content_html = serialize_children(
                find_first(root, class_name="NewsDetails__content")
            )
        content_links, content_images, content_text = extract_content_assets(
            content_html, url
        )
        if status == "ok" and not content_html:
            warnings.append("article has empty content")

        image_info = as_dict(news.get("imageInfo"))
        dom_image = _first_dom_image(root)
        article_image = article_ld.get("image", "")
        if isinstance(article_image, dict):
            article_image = article_image.get("url", "")
        image_url = normalized_url(news.get("image"))
        image_url = image_url or normalized_url(article_image)
        image_url = image_url or normalized_url(meta.get("og:image"))

        author_data = as_dict(page_data.get("author"))
        author_user = as_dict(author_data.get("user"))
        news_author = as_dict(news.get("author"))
        ld_author = _article_author(article_ld)
        author_name = sanitize(author_user.get("login", ""))
        author_name = author_name or sanitize(news_author.get("anchor", ""))
        author_name = author_name or sanitize(ld_author.get("name", ""))
        dom_author = find_first(root, class_name="NewsDetails__author-link")
        author_name = author_name or simple_text(dom_author)
        author_url = normalized_url(news_author.get("href", ""), url)
        author_url = author_url or normalized_url(ld_author.get("url", ""), url)
        if not author_url and dom_author:
            author_url = normalized_url(dom_author.attrs.get("href", ""), url)
        author_description = sanitize(ld_author.get("description", ""))
        dom_author_description = find_first(
            root, class_name="NewsDetails__author-description"
        )
        author_description = author_description or simple_text(dom_author_description)
        author_articles_count = optional_int(page_data.get("authorNewsCounter"))
        if author_articles_count is None and dom_author_description:
            match = AUTHOR_ARTICLES_RE.search(simple_text(dom_author_description))
            if match:
                author_articles_count = int(match.group(1).replace(" ", ""))

        category = as_dict(news.get("category"))
        comments = as_list(page_data.get("comments"))
        comment_ids = loaded_comment_ids(comments)
        comments_meta = as_dict(page_data.get("commentsMeta"))
        emotions = as_list(page_data.get("emotions"))
        comments_total = optional_int(news.get("commentsTotal"))
        if comments_total is None:
            comments_total = optional_int(comments_meta.get("count"))
        comments_new = optional_int(news.get("commentsNew"))
        if comments_new is None:
            comments_new = optional_int(comments_meta.get("newCount"))
        comments_meta_count = optional_int(comments_meta.get("count"))
        comments_complete = (
            status == "ok"
            and comments_meta_count is not None
            and len(comment_ids) == comments_meta_count
        )

        article_name = article_ld.get("headline") or article_ld.get("name")
        title = sanitize(news.get("title", "")) or dom_title
        title = title or sanitize(article_name) or sanitize(meta.get("og:title"))
        if status != "ok" and not news and not dom_title:
            title = ""

        article_author_count = optional_int(page_data.get("authorNewsCounter"))
        if author_articles_count is None:
            author_articles_count = article_author_count

        poster_caption = simple_text(find_first(root, class_name="NewsPoster__caption"))
        row.update(
            {
                "status": status,
                "news_id": decoded_id,
                "error_status_code": error_code,
                "error_message": error_message,
                "slug": slug,
                "url": url,
                "canonical_url": canonical_url,
                "page_title": page_title,
                "title": title,
                "foreword": sanitize(news.get("foreword", "")) or dom_foreword,
                "content_text": content_text,
                "content_html": content_html,
                "published_at": sanitize(news.get("publishedAt", ""))
                or sanitize(article_ld.get("datePublished", "")),
                "modified_at": sanitize(article_ld.get("dateModified", "")),
                "source_site": sanitize(news.get("source", "")),
                "image_url": image_url,
                "image_width": optional_int(image_info.get("width")),
                "image_height": optional_int(image_info.get("height")),
                "image_alt": sanitize(news.get("alt", ""))
                or (sanitize(dom_image.attrs.get("alt", "")) if dom_image else ""),
                "media_source": sanitize(news.get("mediaSource", ""))
                or poster_caption,
                "video_html": sanitize(news.get("video", "")),
                "author_id": optional_int(author_data.get("id")),
                "author_name": author_name,
                "author_url": author_url,
                "author_description": author_description,
                "author_articles_count": author_articles_count,
                "category_title": sanitize(category.get("title", "")),
                "category_slug": sanitize(category.get("alias", "")),
                "comments_total": comments_total,
                "comments_new": comments_new,
                "comments_loaded_count": len(comment_ids),
                "comments_meta_count": comments_meta_count,
                "comments_complete": comments_complete,
                "has_spoilers": bool(comments_meta.get("hasSpoilers", False)),
                "images": json_safe(as_list(news.get("images"))),
                "tags": json_safe(as_list(news.get("tags"))),
                "content_links": json_safe(content_links),
                "content_images": json_safe(content_images),
                "similar_news": json_safe(as_list(page_data.get("similarNews"))),
                "categories": json_safe(as_list(page_data.get("categories"))),
                "comments": json_safe(comments),
                "comments_meta": json_safe(comments_meta),
                "comments_with_images": json_safe(
                    as_list(page_data.get("commentsWithImages"))
                ),
                "read_also_aside": json_safe(
                    as_list(page_data.get("readAlsoAside"))
                ),
                "read_also_main": json_safe(as_list(page_data.get("readAlsoMain"))),
                "emotions": json_safe(emotions),
                "author": json_safe(author_data),
                "catalog_links": json_safe(as_list(page_data.get("catalogLinks"))),
                "seo_meta": meta_records,
                "hreflang_links": hreflang,
                "json_ld": json_ld,
                "nuxt_news": json_safe(news),
                "nuxt_route": route,
                "nuxt_errors": json_safe(
                    {"error": nuxt_error, "_errors": nuxt_internal_errors}
                ),
                "parse_warnings": warnings,
            }
        )
        row.update(_reaction_counts(emotions))
        issue = None
        if status != "ok" or warnings:
            detail = error_message or "; ".join(warnings)
            issue = f"{relative}: {status}" + (f": {detail}" if detail else "")
        return ParseResult(row=row, source_digest=digest, issue=issue)
    except Exception as error:  # one bad page must not remove the other 12k rows
        row.update(
            {
                "status": "parse_error",
                "error_message": f"{type(error).__name__}: {error}",
                "parse_warnings": ["page parsing failed; inspect source HTML"],
                "url": normalized_url(f"/news/{source.news_id}/"),
            }
        )
        return ParseResult(
            row=row,
            source_digest=digest,
            issue=f"{relative}: parse_error: {type(error).__name__}: {error}",
        )


def discover_inputs(
    input_dir: Path,
) -> tuple[list[SourceInput], str, int, int]:
    if not input_dir.is_dir():
        raise ValueError(f"Input directory does not exist: {input_dir}")
    inputs = []
    inventory = hashlib.sha256()
    total_bytes = 0
    seen_ids = set()
    for path in input_dir.iterdir():
        if not path.is_file():
            raise ValueError(f"Unexpected directory in news source: {path}")
        if path.suffix.lower() != ".html" or not path.stem.isdigit():
            raise ValueError(f"Unexpected news source file: {path}")
        news_id = int(path.stem)
        if news_id in seen_ids:
            raise ValueError(f"Duplicate numeric news id: {news_id}")
        seen_ids.add(news_id)
        inputs.append(SourceInput(news_id, path))
    inputs.sort(key=lambda item: (item.news_id, item.path.name))
    for item in inputs:
        stat = item.path.stat()
        relative = item.path.relative_to(input_dir).as_posix()
        inventory.update(relative.encode("utf-8") + b"\0")
        inventory.update(str(stat.st_size).encode("ascii") + b"\0")
        inventory.update(str(stat.st_mtime_ns).encode("ascii") + b"\0")
        total_bytes += stat.st_size
    return inputs, inventory.hexdigest(), len(inputs), total_bytes


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
    expected_min_id: int | None,
    expected_max_id: int | None,
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
        manifest.get("entity") != "myshows_news"
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("fields") != list(FIELDS)
        or manifest.get("extractor_sha256") != extractor_sha256
        or manifest.get("complete") is not True
        or manifest.get("extraction_complete") is not True
        or manifest.get("source_inventory_sha256") != inventory_sha256
        or manifest.get("expected_min_id") != expected_min_id
        or manifest.get("expected_max_id") != expected_max_id
        or manifest.get("jsonl_size_bytes") != output_path.stat().st_size
        or manifest.get("jsonl_sha256") != file_sha256(output_path)
    ):
        return None
    return manifest


def iter_results(
    inputs: list[SourceInput], input_dir: Path, workers: int
) -> Iterable[ParseResult]:
    if workers == 1:
        return (parse_source(item, input_dir) for item in inputs)
    try:
        executor = ProcessPoolExecutor(max_workers=workers)
    except (OSError, PermissionError) as error:
        print(
            f"myshows news: process pool unavailable ({error}); using one worker",
            file=sys.stderr,
            flush=True,
        )
        return (parse_source(item, input_dir) for item in inputs)
    input_iterator = iter(inputs)
    pending = deque()
    for item in input_iterator:
        pending.append(executor.submit(parse_source, item, input_dir))
        if len(pending) >= workers * 2:
            break

    def close_executor() -> Iterator[ParseResult]:
        try:
            while pending:
                future = pending.popleft()
                yield future.result()
                try:
                    item = next(input_iterator)
                except StopIteration:
                    continue
                pending.append(executor.submit(parse_source, item, input_dir))
        finally:
            for future in pending:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)

    return close_executor()


def source_tree_update(digest: Any, relative: str, source_digest: str) -> None:
    digest.update(relative.encode("utf-8"))
    digest.update(b"\0")
    digest.update(bytes.fromhex(source_digest))


def extract_news(
    input_dir: Path,
    output_dir: Path,
    *,
    workers: int = 1,
    force: bool = False,
    progress_every: int = 500,
    expected_min_id: int | None = None,
    expected_max_id: int | None = None,
) -> dict[str, Any]:
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    if workers <= 0 or progress_every < 0:
        raise ValueError("workers must be positive and progress_every non-negative")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "news.jsonl.gz"
    manifest_path = output_dir / "manifest.json"
    output_part = output_dir / "news.jsonl.gz.part"
    manifest_part = output_dir / "manifest.json.part"

    inputs, inventory_digest, source_file_count, source_bytes = discover_inputs(
        input_dir
    )
    source_ids = [item.news_id for item in inputs]
    source_min_id = source_ids[0] if source_ids else None
    source_max_id = source_ids[-1] if source_ids else None
    source_ids_contiguous = bool(source_ids) and source_ids == list(
        range(source_min_id, source_max_id + 1)
    )
    if (expected_min_id is None) != (expected_max_id is None):
        raise ValueError("expected_min_id and expected_max_id must be set together")
    if expected_min_id is not None and expected_max_id is not None:
        if expected_min_id > expected_max_id:
            raise ValueError("expected_min_id must not exceed expected_max_id")
        expected_ids = set(range(expected_min_id, expected_max_id + 1))
        actual_ids = set(source_ids)
        missing_ids = sorted(expected_ids - actual_ids)
        unexpected_ids = sorted(actual_ids - expected_ids)
        if missing_ids or unexpected_ids:
            raise ValueError(
                "news source id coverage mismatch; "
                f"missing={missing_ids[:20]}"
                f"{'...' if len(missing_ids) > 20 else ''}, "
                f"unexpected={unexpected_ids[:20]}"
                f"{'...' if len(unexpected_ids) > 20 else ''}"
            )
    extractor_digest = file_sha256(Path(__file__).resolve())
    if not force:
        cached = cached_manifest(
            output_path,
            manifest_path,
            inventory_digest,
            extractor_digest,
            expected_min_id,
            expected_max_id,
        )
        if cached is not None:
            result = dict(cached)
            result["status"] = "cached"
            return result

    output_part.unlink(missing_ok=True)
    manifest_part.unlink(missing_ok=True)
    statuses: Counter[str] = Counter()
    source_tree = hashlib.sha256()
    issues = []
    previous_id: int | None = None
    record_count = 0
    comments_loaded_count = 0
    comments_meta_count = 0
    comments_incomplete_page_count = 0

    try:
        with output_part.open("wb") as raw_output:
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=raw_output, mtime=0
            ) as compressed:
                for index, result in enumerate(
                    iter_results(inputs, input_dir, workers), 1
                ):
                    row = result.row
                    if set(row) != set(FIELDS):
                        missing = sorted(set(FIELDS) - set(row))
                        extra = sorted(set(row) - set(FIELDS))
                        raise ValueError(
                            f"row {index} field mismatch: missing={missing}, extra={extra}"
                        )
                    source_id = row["source_id"]
                    if previous_id is not None and source_id <= previous_id:
                        raise ValueError("news rows are not strictly sorted by source_id")
                    previous_id = source_id
                    if row["status"] not in STATUSES:
                        raise ValueError(f"unknown row status: {row['status']!r}")
                    statuses[row["status"]] += 1
                    if isinstance(row["comments_loaded_count"], int):
                        comments_loaded_count += row["comments_loaded_count"]
                    if isinstance(row["comments_meta_count"], int):
                        comments_meta_count += row["comments_meta_count"]
                    if row["status"] == "ok" and not row["comments_complete"]:
                        comments_incomplete_page_count += 1
                    if result.issue:
                        issues.append(result.issue)
                    source_tree_update(
                        source_tree, row["source_file"], result.source_digest
                    )
                    compressed.write(
                        json.dumps(
                            row, ensure_ascii=False, separators=(",", ":")
                        ).encode("utf-8")
                        + b"\n"
                    )
                    record_count += 1
                    if progress_every and index % progress_every == 0:
                        print(
                            f"myshows news: processed {index}/{len(inputs)} pages",
                            file=sys.stderr,
                            flush=True,
                        )
            raw_output.flush()
            os.fsync(raw_output.fileno())

        if record_count != source_file_count:
            raise ValueError(
                f"record/source mismatch: {record_count} != {source_file_count}"
            )
        output_digest = file_sha256(output_part)
        manifest: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "extractor_sha256": extractor_digest,
            "entity": "myshows_news",
            "complete": True,
            "extraction_complete": True,
            "source_parse_complete": not any(
                statuses[name]
                for name in ("parse_error", "nuxt_error", "missing_news")
            ),
            "fields": list(FIELDS),
            "record_count": record_count,
            "source_dir": str(input_dir),
            "source_file_count": source_file_count,
            "source_bytes": source_bytes,
            "source_min_id": source_min_id,
            "source_max_id": source_max_id,
            "source_ids_contiguous": source_ids_contiguous,
            "expected_min_id": expected_min_id,
            "expected_max_id": expected_max_id,
            "source_status_counts": dict(sorted(statuses.items())),
            "source_issue_details": issues,
            "comments_loaded_count": comments_loaded_count,
            "comments_meta_count": comments_meta_count,
            "comments_incomplete_page_count": comments_incomplete_page_count,
            "comments_missing_count": comments_meta_count - comments_loaded_count,
            "source_inventory_sha256": inventory_digest,
            "source_inventory_sha256_algorithm": (
                "sha256(path,NUL,size,NUL,mtime_ns,NUL)"
            ),
            "source_tree_sha256": source_tree.hexdigest(),
            "source_tree_sha256_algorithm": "sha256(path,NUL,file_sha256_bytes)",
            "jsonl": output_path.name,
            "jsonl_sha256": output_digest,
            "jsonl_size_bytes": output_part.stat().st_size,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_part.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with manifest_part.open("rb") as file:
            os.fsync(file.fileno())
        os.replace(output_part, output_path)
        os.replace(manifest_part, manifest_path)
        result = dict(manifest)
        result["status"] = "extracted"
        return result
    except BaseException:
        output_part.unlink(missing_ok=True)
        manifest_part.unlink(missing_ok=True)
        raise


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--workers",
        type=int,
        default=min(4, os.cpu_count() or 1),
        help="parallel HTML parser processes (default: up to 4)",
    )
    parser.add_argument("--force", action="store_true", help="ignore valid cache")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=500,
        help="print progress every N pages; 0 disables it",
    )
    parser.add_argument(
        "--expected-min-id",
        type=int,
        default=0,
        help="fail unless this first source id is present (default: 0)",
    )
    parser.add_argument(
        "--expected-max-id",
        type=int,
        default=12558,
        help="fail unless every id through this value is present (default: 12558)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = extract_news(
        args.input_dir,
        args.output_dir,
        workers=args.workers,
        force=args.force,
        progress_every=args.progress_every,
        expected_min_id=args.expected_min_id,
        expected_max_id=args.expected_max_id,
    )
    print(
        f"myshows news: {manifest['status']}; "
        f"rows={manifest['record_count']}; "
        f"output={manifest['jsonl']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
