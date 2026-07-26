#!/usr/bin/env python3
"""Extract anime metadata and statistics from saved Shikimori HTML pages.

The output contains one row per HTML file, including error, redirect, and
age-restricted pages. Repeated and uncommon structures are stored as compact
JSON in dedicated TSV columns so that information is not discarded merely
because it does not fit the common page layout.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlsplit, urlunsplit

from bs4 import BeautifulSoup, FeatureNotFound, Tag


PROJECT_DIR = Path(__file__).resolve().parent
SCORE_KEYS = [str(score) for score in range(10, 0, -1)]
LIST_STATUS_KEYS = ["planned", "watching", "completed", "on_hold", "dropped"]

COLUMNS = [
    # Provenance and page quality.
    "source_file",
    "source_stem",
    "source_requested_id",
    "source_variant",
    "source_prefix",
    "source_size_bytes",
    "page_status",
    "parse_error",
    "warnings",
    "server_time",
    "locale",
    # Identifiers and URLs.
    "canonical_id",
    "canonical_slug",
    "canonical_url",
    "canonical_url_raw",
    "entry_id",
    "entry_json",
    "redirect_id",
    "redirect_url",
    "discussion_url",
    "discussion_topic_id",
    "other_names_url",
    "resources_url",
    "watch_online_url",
    # Titles and page-level metadata.
    "html_title",
    "title",
    "title_russian",
    "title_heading",
    "alternative_titles",
    "is_adult",
    "og_type",
    # Anime facts.
    "anime_type",
    "episodes",
    "episodes_aired",
    "episodes_total",
    "episodes_text",
    "episode_duration",
    "duration_seconds",
    "status_code",
    "status_marker",
    "status_text",
    "status_dates",
    "aired_on",
    "aired_on_kind",
    "released_on",
    "first_episode_text",
    "first_episode_at",
    "next_episode_text",
    "next_episode_at",
    "age_rating",
    "age_rating_text",
    "age_rating_details",
    "source_material",
    "licensors",
    "russian_licensed_name",
    "russian_premiere",
    "anime_notice",
    "additional_info",
    # Classification and editorial text.
    "genres_en",
    "genres_ru",
    "genre_ids",
    "genres_json",
    "themes_en",
    "themes_ru",
    "theme_ids",
    "themes_json",
    "studios",
    "studio_ids",
    "studios_json",
    "has_description",
    "description",
    "description_notice",
    "description_authors",
    "description_source_json",
    "description_links_json",
    "description_other",
    "description_other_source_json",
    "description_other_links_json",
    # Aggregate rating and distributions.
    "score",
    "score_label",
    "rating_count",
    "best_rating",
] + [f"score_{score}_count" for score in SCORE_KEYS] + [
    "score_votes_total",
    "score_distribution_mean",
] + [f"list_{status}_count" for status in LIST_STATUS_KEYS] + [
    "user_list_total",
    "user_list_stats_total",
    "favorites_count",
    "favorites_preview_count",
    "comments_count",
    "comments_preview_count",
    "reviews_count",
    "reviews_positive_count",
    "reviews_neutral_count",
    "reviews_negative_count",
    "clubs_count",
    "clubs_preview_count",
    "collections_count",
    "collections_preview_count",
    "news_count",
    "news_preview_count",
    # External identifiers and media.
    "myanimelist_id",
    "anidb_id",
    "world_art_id",
    "anime_news_network_id",
    "kinopoisk_id",
    "official_site_url",
    "wikipedia_url",
    "twitter_url",
    "poster_url",
    "poster_id",
    "poster_original_url",
    "poster_preview_url",
    "poster_width",
    "poster_height",
    "poster_resolution",
    "poster_resolution_width",
    "poster_resolution_height",
    "og_image_url",
    "video_release_date",
    "video_duration_seconds",
    "video_tags",
    # Lossless-ish structured fields for uncommon/repeated page content.
    "raw_info_json",
    "additional_links_json",
    "breadcrumbs_json",
    "related_json",
    "authors_json",
    "characters_json",
    "similar_json",
    "videos_json",
    "dubbing_teams",
    "subtitles",
    "comments_json",
    "news_json",
    "clubs_json",
    "collections_json",
    "favorites_json",
    "external_links_json",
    "info_endpoints_json",
    "sections_json",
    "meta_json",
]

INFO_LABELS = {
    "anime_type": ("Тип",),
    "episodes_text": ("Эпизоды",),
    "episode_duration": ("Длительность эпизода",),
    "status_text": ("Статус",),
    "age_rating": ("Рейтинг",),
    "source_material": ("Первоисточник", "Источник"),
    "licensors": ("Лицензировано",),
    "russian_licensed_name": ("Лицензировано в РФ под названием",),
    "russian_premiere": ("Премьера в РФ",),
    "anime_notice": ("У аниме",),
    "additional_info": ("Доп. информация",),
    "first_episode_text": ("Первый эпизод",),
    "next_episode_text": ("Следующий эпизод",),
}

EXTERNAL_ID_PATTERNS = {
    "myanimelist_id": re.compile(r"myanimelist\.net/anime/(\d+)", re.I),
    "anidb_id": re.compile(r"(?:anidb\.net/(?:anime/)?|aid=)(\d+)", re.I),
    "world_art_id": re.compile(r"world-art\.ru/animation/(?:animation\.php\?id=)?(\d+)", re.I),
    "anime_news_network_id": re.compile(
        r"animenewsnetwork\.com/encyclopedia/anime\.php\?id=(\d+)", re.I
    ),
    "kinopoisk_id": re.compile(r"kinopoisk\.ru/(?:film|series)/(\d+)", re.I),
}


def normalize_text(value: Any) -> str:
    """Collapse whitespace and remove characters that can break a TSV row."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFC", str(value)).replace("\x00", " ")
    return re.sub(r"\s+", " ", text).strip()


def node_text(node: Tag | None) -> str:
    return normalize_text(node.get_text(" ", strip=True)) if node else ""


def compact_json(value: Any) -> str:
    if value in (None, [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def first_attr(node: Tag | None, *names: str) -> str:
    if not node:
        return ""
    for name in names:
        value = node.get(name)
        if value:
            return normalize_text(value)
    return ""


def first_meta(
    soup: BeautifulSoup, attribute: str, value: str, *, scope: Tag | None = None
) -> str:
    container = scope or soup
    node = container.find("meta", attrs={attribute: value})
    return first_attr(node, "content")


def first_direct_meta(scope: Tag | None, itemprop: str) -> str:
    if not scope:
        return ""
    node = scope.find("meta", attrs={"itemprop": itemprop}, recursive=False)
    return first_attr(node, "content")


def json_attribute(node: Tag | None, name: str) -> dict[str, Any]:
    if not node:
        return {}
    try:
        value = json.loads(node.get(name, "{}"))
        return value if isinstance(value, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def clean_url(url: str) -> str:
    """Remove accidental locale/query fragments from canonical URLs."""
    if not url:
        return ""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def anime_id_from_url(url: str) -> str:
    if not url:
        return ""
    match = re.search(r"/animes/[A-Za-z]?(\d+)(?:-|/?$)", urlsplit(url).path)
    return match.group(1) if match else ""


def integer(value: Any) -> int | str:
    text = normalize_text(value).replace(" ", "")
    match = re.search(r"-?\d+", text)
    return int(match.group()) if match else ""


def source_identity(stem: str) -> tuple[str, str, str]:
    numeric = re.fullmatch(r"(\d+)", stem)
    if numeric:
        return numeric.group(1), "numeric", ""
    prefixed = re.fullmatch(r"([A-Za-z]+)(\d+)", stem)
    if prefixed:
        return prefixed.group(2), "prefixed", prefixed.group(1)
    return "", "named", ""


def empty_row(path: Path, root: Path) -> dict[str, Any]:
    relative = path.relative_to(root).as_posix()
    requested_id, variant, prefix = source_identity(path.stem)
    row: dict[str, Any] = {column: "" for column in COLUMNS}
    row.update(
        {
            "source_file": relative,
            "source_stem": path.stem,
            "source_requested_id": requested_id,
            "source_variant": variant,
            "source_prefix": prefix,
        }
    )
    return row


def parse_soup(raw: bytes) -> BeautifulSoup:
    try:
        return BeautifulSoup(raw, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(raw, "html.parser")


def classify_page(soup: BeautifulSoup, raw_text: str) -> str:
    body_classes = set(soup.body.get("class", [])) if soup.body else set()
    h1 = " ".join(node_text(node) for node in soup.find_all("h1"))
    error_code = node_text(soup.select_one(".error-404"))
    if "p-age_restricted" in body_classes or "ограничен 18+" in raw_text:
        return "age_restricted"
    if soup.find(attrs={"itemprop": "headline"}) and "p-animes-show" in body_classes:
        return "ok"
    if error_code == "302" or "Страница переехала" in h1:
        return "redirect"
    if (
        error_code == "404"
        or "Страница не найдена" in h1
        or (soup.title and node_text(soup.title) == "404")
    ):
        return "not_found"
    if normalize_text(soup.get_text(" ", strip=True)) == "Retry later":
        return "retry_later"
    if "p-animes-show" in body_classes:
        return "incomplete"
    return "unknown"


def info_records(soup: BeautifulSoup) -> list[dict[str, Any]]:
    records = []
    for line in soup.select(".b-entry-info .line"):
        key_node = line.select_one(".key")
        value_node = line.select_one(".value")
        if not key_node or not value_node:
            continue
        key = node_text(key_node).rstrip(":").replace("\xa0", " ").strip()
        links = []
        for link in value_node.select("a, [data-href]"):
            url = first_attr(link, "href", "data-href")
            text = node_text(link) or first_attr(link, "title", "data-text")
            if url or text:
                links.append({"text": text, "url": url})
        tags = [node_text(tag) for tag in value_node.select(".b-tag") if node_text(tag)]
        records.append(
            {
                "key": key,
                "value": node_text(value_node),
                "links": links,
                "tags": tags,
            }
        )
    return records


def values_by_label(records: list[dict[str, Any]], labels: Iterable[str]) -> str:
    wanted = set(labels)
    values = [record["value"] for record in records if record["key"] in wanted]
    return " | ".join(value for value in values if value)


def record_by_label(
    records: list[dict[str, Any]], labels: Iterable[str]
) -> dict[str, Any] | None:
    wanted = set(labels)
    return next((record for record in records if record["key"] in wanted), None)


def taxonomy_from_line(line: Tag | None) -> tuple[str, str, str, str]:
    values = []
    if line:
        for link in line.select(".value a"):
            url = first_attr(link, "href", "data-href")
            match = re.search(r"/genre/(\d+)-", url)
            values.append(
                {
                    "id": match.group(1) if match else "",
                    "name_en": node_text(link.select_one(".genre-en")),
                    "name_ru": node_text(link.select_one(".genre-ru")),
                    "text": node_text(link),
                    "url": url,
                }
            )
    return _taxonomy_columns(values)


def _taxonomy_columns(values: list[dict[str, str]]) -> tuple[str, str, str, str]:
    english = " | ".join(value["name_en"] or value["text"] for value in values)
    russian = " | ".join(value["name_ru"] for value in values if value["name_ru"])
    ids = " | ".join(value["id"] for value in values if value["id"])
    return english, russian, ids, compact_json(values)


def info_line(soup: BeautifulSoup, labels: Iterable[str]) -> Tag | None:
    wanted = set(labels)
    for line in soup.select(".b-entry-info .line"):
        key = node_text(line.select_one(".key")).rstrip(":").strip()
        if key in wanted:
            return line
    return None


def extract_studios(soup: BeautifulSoup) -> tuple[str, str, str]:
    studios = []
    seen = set()
    for link in soup.select('.c-info-right a[href*="/animes/studio/"]'):
        url = first_attr(link, "href")
        if url in seen:
            continue
        seen.add(url)
        match = re.search(r"/studio/(\d+)-", url)
        image = link.find("img")
        name = first_attr(link, "title", "data-text")
        name = re.sub(r"^Аниме студии\s+", "", name)
        if not name and image:
            name = re.sub(r"^Аниме студии\s+", "", first_attr(image, "alt"))
        studios.append(
            {
                "id": match.group(1) if match else "",
                "name": name or node_text(link),
                "url": url,
                "logo_url": first_attr(image, "src"),
            }
        )
    return (
        " | ".join(studio["name"] for studio in studios if studio["name"]),
        " | ".join(studio["id"] for studio in studios if studio["id"]),
        compact_json(studios),
    )


def parse_stats(node: Tag | None) -> dict[str, int]:
    if not node:
        return {}
    try:
        data = json.loads(node.get("data-stats", "[]"))
        return {normalize_text(key): int(value) for key, value in data}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def extract_external_links(soup: BeautifulSoup) -> list[dict[str, str]]:
    result = []
    for container in soup.select(".b-external_link"):
        classes = [
            value
            for value in container.get("class", [])
            if value not in {"b-external_link", "b-menu-line"}
        ]
        link = container.select_one("[data-href], a[href]")
        url = first_attr(link, "data-href", "href")
        result.append(
            {
                "kind": classes[0] if classes else "",
                "name": node_text(link or container),
                "url": url,
            }
        )
    return result


def variant_item(item: Tag) -> dict[str, Any]:
    roles = [node_text(tag) for tag in item.select(".line .b-tag") if node_text(tag)]
    tags = [node_text(tag) for tag in item.select(".value > .b-tag") if node_text(tag)]
    relation = node_text(item.select_one(".b-anime_status_tag.other"))
    image = item.select_one("img")
    return {
        "id": first_attr(item, "data-id"),
        "type": first_attr(item, "data-type"),
        "url": first_attr(item, "data-url"),
        "name_en": node_text(item.select_one(".name-en")),
        "name_ru": node_text(item.select_one(".name-ru")),
        "text": first_attr(item, "data-text"),
        "roles": roles,
        "tags": tags,
        "relation": relation,
        "image_url": first_attr(image, "src"),
    }


def section_container(soup: BeautifulSoup, title: str) -> Tag | None:
    for heading in soup.select(".subheadline"):
        if node_text(heading).startswith(title):
            return heading.parent if isinstance(heading.parent, Tag) else None
    return None


def extract_related(soup: BeautifulSoup) -> list[dict[str, Any]]:
    container = section_container(soup, "Связанное")
    if not container:
        return []
    return [
        variant_item(item)
        for item in container.select(".b-db_entry-variant-list_item[data-type]")
    ]


def extract_authors(soup: BeautifulSoup) -> list[dict[str, Any]]:
    return [variant_item(item) for item in soup.select(".c-authors .b-db_entry-variant-list_item")]


def extract_characters(soup: BeautifulSoup) -> list[dict[str, str]]:
    characters = []
    for container in soup.select(".c-characters"):
        heading = container.select_one(".subheadline")
        section = node_text(heading)
        for item in container.select("article.c-character"):
            link = item.select_one("a.cover[href]")
            image_meta = item.find("meta", attrs={"itemprop": "image"})
            characters.append(
                {
                    "id": first_attr(item, "id"),
                    "section": section,
                    "name_en": node_text(item.select_one(".name-en")),
                    "name_ru": node_text(item.select_one(".name-ru")),
                    "url": first_attr(link, "href"),
                    "image_url": first_attr(image_meta, "content"),
                }
            )
    return characters


def extract_similar(soup: BeautifulSoup) -> list[dict[str, str]]:
    result = []
    for item in soup.select(".cc-similar article.b-catalog_entry"):
        link = item.select_one("a.cover[href]")
        image = item.find("meta", attrs={"itemprop": "image"}, recursive=False)
        created = item.find("meta", attrs={"itemprop": "dateCreated"}, recursive=False)
        result.append(
            {
                "id": first_attr(item, "id"),
                "name_en": node_text(item.select_one(".name-en")),
                "name_ru": node_text(item.select_one(".name-ru")),
                "url": first_attr(link, "href"),
                "image_url": first_attr(image, "content"),
                "aired_on": first_attr(created, "content"),
            }
        )
    return result


def extract_videos(soup: BeautifulSoup) -> list[dict[str, str]]:
    videos = []
    for item in soup.select(".c-videos .b-video"):
        link = item.select_one(".video-link")
        image = item.select_one("img")
        providers = [
            value
            for value in item.get("class", [])
            if value not in {"b-video", "unprocessed", "c-video"}
            and not value.startswith("entry-")
        ]
        videos.append(
            {
                "name": node_text(item.select_one(".name")),
                "kind": node_text(item.select_one(".marker")),
                "provider": providers[0] if providers else "",
                "url": first_attr(link, "href"),
                "embed_url": first_attr(link, "data-href"),
                "preview_url": first_attr(image, "src"),
            }
        )
    return videos


def extract_menu_values(soup: BeautifulSoup, title: str) -> str:
    container = section_container(soup, title)
    if not container:
        return ""
    values = [node_text(node) for node in container.select(".b-menu-line")]
    return " | ".join(value for value in values if value)


def description_source(container: Tag | None) -> dict[str, Any]:
    if not container:
        return {}
    source = container.select_one(".b-source .source .val")
    source_link = source.select_one("a[href]") if source else None
    contributors = []
    for contributor in container.select(".b-source .contributors .b-user16"):
        link = contributor.select_one("a[href]")
        contributors.append(
            {
                "name": node_text(link or contributor),
                "url": first_attr(link, "href"),
            }
        )
    result = {
        "source": node_text(source),
        "source_url": first_attr(source_link, "href"),
        "contributors": contributors,
    }
    return result if any(result.values()) else {}


def text_links(container: Tag | None) -> list[dict[str, str]]:
    if not container:
        return []
    return [
        {"text": node_text(link), "url": first_attr(link, "href")}
        for link in container.select("a[href]")
    ]


def extract_comments(soup: BeautifulSoup) -> list[dict[str, Any]]:
    comments = []
    for item in soup.select('.b-comment[itemprop="comment"]'):
        body = item.select_one('.inner > .body[itemprop="text"]')
        author = item.select_one("header a.name")
        created = item.select_one("header time[datetime]")
        parent = item.find_parent(class_="b-comment")
        comments.append(
            {
                "id": first_attr(item, "id", "data-track_comment"),
                "parent_id": first_attr(parent, "id"),
                "user_id": first_attr(item, "data-user_id"),
                "user": node_text(author) or first_attr(item, "data-user_nickname"),
                "user_url": first_attr(author, "href"),
                "created_at": first_attr(created, "datetime"),
                "text": node_text(body),
                "links": text_links(body),
            }
        )
    return comments


def extract_news(soup: BeautifulSoup) -> list[dict[str, str]]:
    container = section_container(soup, "Новости")
    if not container:
        return []
    result = []
    seen = set()
    for item in container.select(".b-menu-line.entry"):
        url = first_attr(item, "href", "data-href")
        name = node_text(item.select_one(".name")) or node_text(item)
        key = (url, name)
        if key in seen:
            continue
        seen.add(key)
        time_node = item.select_one("time")
        result.append(
            {
                "name": name,
                "url": url,
                "datetime": first_attr(time_node, "datetime"),
                "date_text": node_text(time_node),
            }
        )
    return result


def extract_clubs(soup: BeautifulSoup) -> list[dict[str, str]]:
    result = []
    for item in soup.select(".b-clubs .b-club"):
        link = item.select_one("a[href]")
        image = item.select_one("img")
        result.append(
            {
                "id": first_attr(item, "id"),
                "name": first_attr(link, "title") or first_attr(image, "alt"),
                "url": first_attr(link, "href"),
                "image_url": first_attr(image, "src"),
            }
        )
    return result


def extract_collections(soup: BeautifulSoup) -> list[dict[str, Any]]:
    container = section_container(soup, "В коллекциях")
    if not container:
        return []
    result = []
    seen = set()
    for link in container.select('a[href*="/collections/"]'):
        url = first_attr(link, "href")
        if url in seen:
            continue
        seen.add(url)
        match = re.search(r"/collections/(\d+)-", url)
        line = link.find_parent(class_="b-menu-line")
        result.append(
            {
                "id": match.group(1) if match else "",
                "name": node_text(link),
                "url": url,
                "contains_spoilers": int(
                    bool(line and "is-spoilers" in line.get("class", []))
                ),
            }
        )
    return result


def extract_favorites(soup: BeautifulSoup) -> list[dict[str, str]]:
    result = []
    for item in soup.select(".b-favoured .b-user"):
        link = item.select_one("a[href]")
        image = item.select_one("img")
        result.append(
            {
                "id": first_attr(item, "id"),
                "name": first_attr(link, "title") or first_attr(image, "alt"),
                "url": first_attr(link, "href"),
            }
        )
    return result


def extract_sections(soup: BeautifulSoup) -> list[dict[str, Any]]:
    sections = []
    for heading in soup.select(".subheadline"):
        text = node_text(heading)
        link = heading.select_one("a[href], [data-href]")
        count_node = heading.select_one(".count")
        count: int | str = integer(node_text(count_node)) if count_node else ""
        if count == "":
            trailing = re.search(r"\s(\d+)$", text)
            count = int(trailing.group(1)) if trailing else ""
        sections.append(
            {
                "name": text,
                "count": count,
                "url": first_attr(link, "href", "data-href"),
            }
        )
    return sections


def section_count(
    soup: BeautifulSoup, title: str, *, fallback: int = 0
) -> int | str:
    for heading in soup.select(".subheadline"):
        text = node_text(heading)
        if text == title or text.startswith(title + " "):
            count_node = heading.select_one(".count")
            if count_node:
                value = integer(node_text(count_node))
                return value if value != "" else fallback
            match = re.search(r"\s(\d+)$", text)
            return int(match.group(1)) if match else fallback
    return ""


def extract_info_endpoints(soup: BeautifulSoup) -> list[dict[str, str]]:
    result = []
    for line in soup.select(".b-entry-info .line"):
        key = node_text(line.select_one(".key")).rstrip(":").strip()
        for node in line.select("[data-clickloaded-url], [data-postloaded-url]"):
            url = first_attr(node, "data-clickloaded-url", "data-postloaded-url")
            if url:
                result.append({"key": key, "url": url})
    return result


def extract_additional_links(soup: BeautifulSoup) -> list[dict[str, Any]]:
    result = []
    for line in soup.select(".additional-links .line-container"):
        key_node = line.select_one(".key")
        key = node_text(key_node).rstrip(":").strip()
        links = []
        for link in line.select("a[href], [data-href]"):
            links.append(
                {
                    "text": node_text(link),
                    "url": first_attr(link, "href", "data-href"),
                }
            )
        value = " | ".join(link["text"] for link in links if link["text"])
        result.append({"key": key, "value": value, "links": links})
    return result


def extract_meta(soup: BeautifulSoup) -> list[dict[str, str]]:
    result = []
    for meta in soup.select("head meta[property], head meta[name]"):
        namespace = "property" if meta.get("property") else "name"
        key = first_attr(meta, namespace)
        if not key.startswith(("og:", "video:", "twitter:")):
            continue
        result.append({"key": key, "value": first_attr(meta, "content")})
    return result


def parse_html(path: Path, root: Path, raw: bytes) -> dict[str, Any]:
    row = empty_row(path, root)
    row["source_size_bytes"] = len(raw)
    raw_text = raw.decode("utf-8", errors="replace")
    soup = parse_soup(raw)
    status = classify_page(soup, raw_text)
    row["page_status"] = status
    row["html_title"] = node_text(soup.title)
    row["title_heading"] = node_text(soup.find("h1"))

    body = soup.body
    if body:
        row["server_time"] = first_attr(body, "data-server_time")
        row["locale"] = first_attr(body, "data-locale")

    redirect_link = None
    if status == "redirect":
        redirect_link = soup.find("a", href=re.compile(r"/animes/")) or soup.find("a", href=True)
        row["redirect_url"] = first_attr(redirect_link, "href")
        row["redirect_id"] = anime_id_from_url(row["redirect_url"])
        return row

    if status != "ok":
        if status == "age_restricted":
            row["is_adult"] = 1
        return row

    headline_node = soup.find("meta", attrs={"itemprop": "headline"})
    main_scope = soup.select_one(".l-content > .block")
    if not main_scope and headline_node and isinstance(headline_node.parent, Tag):
        main_scope = headline_node.parent
    canonical_node = soup.find("link", rel=lambda value: value and "canonical" in value)
    canonical_raw = first_attr(canonical_node, "href") or first_direct_meta(
        main_scope, "url"
    )
    canonical_url = clean_url(canonical_raw)
    canonical_slug = urlsplit(canonical_url).path.rstrip("/").split("/")[-1]
    entry_node = soup.select_one(".b-user_rate[data-entry]")
    entry = json_attribute(entry_node, "data-entry")
    entry_id = normalize_text(entry.get("id", ""))
    canonical_id = anime_id_from_url(canonical_url) or entry_id
    created_on = first_direct_meta(main_scope, "dateCreated")
    published_on = first_direct_meta(main_scope, "datePublished")
    row.update(
        {
            "canonical_id": canonical_id,
            "canonical_slug": canonical_slug,
            "canonical_url": canonical_url,
            "canonical_url_raw": canonical_raw,
            "entry_id": entry_id,
            "entry_json": compact_json(entry),
            "title": first_direct_meta(main_scope, "headline")
            or first_meta(soup, "itemprop", "headline")
            or first_meta(soup, "itemprop", "name"),
            "title_russian": first_direct_meta(main_scope, "alternativeHeadline"),
            "aired_on": created_on or published_on,
            "aired_on_kind": "dateCreated" if created_on else (
                "datePublished" if published_on else ""
            ),
            "og_type": first_meta(soup, "property", "og:type"),
            "og_image_url": first_meta(soup, "property", "og:image"),
            "video_release_date": first_meta(soup, "property", "video:release_date"),
            "video_duration_seconds": integer(
                first_meta(soup, "property", "video:duration")
            ),
            "is_adult": int(
                bool(
                    re.search(r"(?:^|\s)18\+(?:\s|$)", row["title_heading"])
                    or "18+" in set(soup.body.get("class", []) if soup.body else [])
                )
            ),
        }
    )
    row["released_on"] = row["video_release_date"]
    row["duration_seconds"] = row["video_duration_seconds"]

    topic = soup.select_one('.subheadline a[title="Все комментарии"]')
    if not topic:
        topic = soup.select_one('a[href*="/forum/animanga/anime-"]')
    row["discussion_url"] = first_attr(topic, "href", "data-href")
    topic_match = re.search(r"/(\d+)-[^/]*$", urlsplit(row["discussion_url"]).path)
    row["discussion_topic_id"] = topic_match.group(1) if topic_match else ""

    other_names = soup.select_one(".other-names")
    row["other_names_url"] = first_attr(
        other_names, "data-clickloaded-url", "data-postloaded-url"
    )
    row["resources_url"] = first_attr(
        soup.select_one(".resources-loader"), "data-postloaded-url"
    )
    row["watch_online_url"] = first_attr(
        soup.select_one(".watch-online"), "data-postloaded-url"
    )

    records = info_records(soup)
    for column, labels in INFO_LABELS.items():
        row[column] = values_by_label(records, labels)
    row["raw_info_json"] = compact_json(records)
    additional_links = extract_additional_links(soup)
    row["additional_links_json"] = compact_json(additional_links)
    anime_notices = [
        item["value"] for item in additional_links if item["key"] == "У аниме"
    ]
    if anime_notices:
        row["anime_notice"] = " | ".join(anime_notices)
    episodes_text = normalize_text(row["episodes_text"])
    aired_match = re.fullmatch(r"(\d+)\s*/\s*(\d+|\?)", episodes_text)
    single_match = re.fullmatch(r"\d+", episodes_text)
    entry_total = integer(entry.get("episodes", ""))
    if entry_total == 0:
        entry_total = ""
    if aired_match:
        row["episodes_aired"] = int(aired_match.group(1))
        visible_total = (
            int(aired_match.group(2)) if aired_match.group(2).isdigit() else ""
        )
    elif single_match:
        row["episodes_aired"] = int(episodes_text)
        visible_total = int(episodes_text)
    else:
        visible_total = ""
    row["episodes_total"] = entry_total or visible_total
    row["episodes"] = row["episodes_total"]

    alternative_record = record_by_label(records, ("Альтернативные названия",))
    if alternative_record:
        alternative = alternative_record["value"].replace("·", "").strip()
        row["alternative_titles"] = alternative

    genre_columns = taxonomy_from_line(info_line(soup, ("Жанр", "Жанры")))
    theme_columns = taxonomy_from_line(info_line(soup, ("Тема", "Темы")))
    (
        row["genres_en"],
        row["genres_ru"],
        row["genre_ids"],
        row["genres_json"],
    ) = genre_columns
    (
        row["themes_en"],
        row["themes_ru"],
        row["theme_ids"],
        row["themes_json"],
    ) = theme_columns

    row["studios"], row["studio_ids"], row["studios_json"] = extract_studios(soup)

    status_line = info_line(soup, ("Статус",))
    status_tag = status_line.select_one(".b-anime_status_tag") if status_line else None
    if status_tag:
        status_classes = [
            value
            for value in status_tag.get("class", [])
            if value not in {"b-anime_status_tag", "linkeable"}
        ]
        row["status_code"] = status_classes[0] if status_classes else ""
        row["status_marker"] = first_attr(status_tag, "data-text", "title")
    if status_line:
        status_details = status_line.select_one(".value [title]")
        row["status_dates"] = first_attr(status_details, "title")
    if (
        row["episodes_aired"] == ""
        and row["episodes_total"] != ""
        and row["status_code"] == "released"
    ):
        row["episodes_aired"] = row["episodes_total"]

    for prefix, label in (
        ("first_episode", "Первый эпизод"),
        ("next_episode", "Следующий эпизод"),
    ):
        episode_line = info_line(soup, (label,))
        episode_time = episode_line.select_one("[data-datetime], time[datetime]") if episode_line else None
        row[f"{prefix}_at"] = first_attr(episode_time, "data-datetime", "datetime")

    age_line = info_line(soup, ("Рейтинг",))
    row["age_rating_text"] = row["age_rating"]
    if age_line:
        rating_node = age_line.select_one(".value .b-tooltipped")
        row["age_rating"] = node_text(rating_node) or row["age_rating_text"]
        row["age_rating_details"] = first_attr(rating_node, "title")
    if "достижению 18 лет" in row["age_rating_text"]:
        row["is_adult"] = 1

    current_description = soup.select_one(".description-current")
    current_text = (
        current_description.select_one('[itemprop="description"]')
        if current_description
        else None
    )
    if current_text and not current_text.select_one(".b-nothing_here"):
        description_text = node_text(current_text)
        if description_text != "Нет описания":
            row["description"] = description_text
    row["has_description"] = int(bool(row["description"]))
    row["description_notice"] = node_text(soup.select_one(".c-description .text-red"))
    if current_description:
        authors = []
        for author in current_description.select(".b-source .b-user16"):
            link = author.select_one("a")
            authors.append(node_text(link or author))
        row["description_authors"] = " | ".join(author for author in authors if author)
        row["description_source_json"] = compact_json(
            description_source(current_description)
        )
        row["description_links_json"] = compact_json(text_links(current_text))

    other_description = soup.select_one(".description-other")
    other_text = other_description.select_one(".text") if other_description else None
    if other_text and not other_text.select_one(".b-nothing_here"):
        other_value = node_text(other_text)
        if other_value != "Нет описания":
            row["description_other"] = other_value
    row["description_other_source_json"] = compact_json(
        description_source(other_description)
    )
    row["description_other_links_json"] = compact_json(text_links(other_text))

    rating_scope = soup.find(attrs={"itemprop": "aggregateRating"})
    row["score"] = first_meta(soup, "itemprop", "ratingValue", scope=rating_scope)
    row["rating_count"] = integer(
        first_meta(soup, "itemprop", "ratingCount", scope=rating_scope)
    )
    row["best_rating"] = first_meta(soup, "itemprop", "bestRating", scope=rating_scope)
    row["score_label"] = node_text(soup.select_one(".b-rate .score-notice"))

    score_stats_node = soup.select_one("#rates_scores_stats")
    score_stats = parse_stats(score_stats_node)
    score_stats_available = bool(
        score_stats_node and score_stats_node.get("data-stats") not in (None, "null")
    )
    for score in SCORE_KEYS:
        row[f"score_{score}_count"] = (
            score_stats.get(score, 0) if score_stats_available else ""
        )
    if score_stats_available:
        row["score_votes_total"] = sum(score_stats.values())
        if row["score_votes_total"]:
            weighted = sum(int(score) * count for score, count in score_stats.items())
            row["score_distribution_mean"] = round(
                weighted / row["score_votes_total"], 4
            )

    list_stats_node = soup.select_one("#rates_statuses_stats")
    list_stats = parse_stats(list_stats_node)
    list_stats_available = bool(
        list_stats_node and list_stats_node.get("data-stats") not in (None, "null")
    )
    for list_status in LIST_STATUS_KEYS:
        row[f"list_{list_status}_count"] = (
            list_stats.get(list_status, 0) if list_stats_available else ""
        )
    row["user_list_stats_total"] = (
        sum(list_stats.values()) if list_stats_available else ""
    )
    row["user_list_total"] = integer(node_text(soup.select_one(".total-rates")))
    if row["user_list_total"] == "" and list_stats_available:
        row["user_list_total"] = row["user_list_stats_total"]

    comments = extract_comments(soup)
    news = extract_news(soup)
    clubs = extract_clubs(soup)
    collections = extract_collections(soup)
    favorites = extract_favorites(soup)
    row["comments_preview_count"] = len(comments)
    row["news_preview_count"] = len(news)
    row["clubs_preview_count"] = len(clubs)
    row["collections_preview_count"] = len(collections)
    row["favorites_preview_count"] = len(favorites)
    row["comments_count"] = section_count(
        soup, "Комментарии", fallback=len(comments)
    ) or 0
    row["news_count"] = section_count(
        soup, "Новости", fallback=len(news)
    ) or 0
    row["clubs_count"] = section_count(
        soup, "В клубах", fallback=len(clubs)
    ) or 0
    row["collections_count"] = section_count(
        soup, "В коллекциях", fallback=len(collections)
    ) or 0
    row["favorites_count"] = section_count(
        soup, "В избранном", fallback=len(favorites)
    ) or 0

    review_nodes = soup.select(".b-reviews_navigation .navigation-node")
    for review in review_nodes:
        opinion = first_attr(review, "data-opinion")
        count = integer(node_text(review.select_one(".count")))
        column = {
            "": "reviews_count",
            "positive": "reviews_positive_count",
            "neutral": "reviews_neutral_count",
            "negative": "reviews_negative_count",
        }.get(opinion)
        if column:
            row[column] = count

    poster = soup.select_one(".b-db_entry-poster")
    poster_meta = poster.find("meta", attrs={"itemprop": "image"}) if poster else None
    poster_image = poster.find("img") if poster else None
    row["poster_url"] = first_attr(poster_meta, "content")
    row["poster_id"] = first_attr(poster, "data-poster_id")
    row["poster_original_url"] = first_attr(poster, "data-href")
    row["poster_preview_url"] = first_attr(poster_image, "src")
    row["poster_width"] = first_attr(poster_image, "width")
    row["poster_height"] = first_attr(poster_image, "height")
    row["poster_resolution"] = node_text(poster.select_one(".marker-text")) if poster else ""
    resolution_match = re.fullmatch(r"(\d+)\s*[xх×]\s*(\d+)", row["poster_resolution"])
    if resolution_match:
        row["poster_resolution_width"] = int(resolution_match.group(1))
        row["poster_resolution_height"] = int(resolution_match.group(2))

    external_links = extract_external_links(soup)
    row["external_links_json"] = compact_json(external_links)
    for external in external_links:
        url = external["url"]
        for column, pattern in EXTERNAL_ID_PATTERNS.items():
            if row[column] == "":
                match = pattern.search(url)
                if match:
                    row[column] = match.group(1)
        kind = external["kind"]
        if kind == "official_site" and not row["official_site_url"]:
            row["official_site_url"] = url
        elif kind == "wikipedia" and not row["wikipedia_url"]:
            row["wikipedia_url"] = url
        elif kind == "twitter" and not row["twitter_url"]:
            row["twitter_url"] = url

    row["video_tags"] = " | ".join(
        first_attr(meta, "content")
        for meta in soup.find_all("meta", attrs={"property": "video:tag"})
        if first_attr(meta, "content")
    )
    row["breadcrumbs_json"] = compact_json(
        [
            {"name": node_text(link), "url": first_attr(link, "href")}
            for link in soup.select(".b-breadcrumbs a[href]")
        ]
    )
    row["related_json"] = compact_json(extract_related(soup))
    row["authors_json"] = compact_json(extract_authors(soup))
    row["characters_json"] = compact_json(extract_characters(soup))
    row["similar_json"] = compact_json(extract_similar(soup))
    row["videos_json"] = compact_json(extract_videos(soup))
    row["dubbing_teams"] = extract_menu_values(soup, "Озвучка")
    row["subtitles"] = extract_menu_values(soup, "Субтитры")
    row["comments_json"] = compact_json(comments)
    row["news_json"] = compact_json(news)
    row["clubs_json"] = compact_json(clubs)
    row["collections_json"] = compact_json(collections)
    row["favorites_json"] = compact_json(favorites)
    row["info_endpoints_json"] = compact_json(extract_info_endpoints(soup))
    row["sections_json"] = compact_json(extract_sections(soup))
    row["meta_json"] = compact_json(extract_meta(soup))

    warnings = []
    if not row["canonical_id"]:
        warnings.append("missing_canonical_id")
    if not row["title"]:
        warnings.append("missing_title")
    requested_id = row["source_requested_id"]
    if requested_id and canonical_id and requested_id != canonical_id:
        warnings.append("source_and_canonical_id_differ")
    row["warnings"] = " | ".join(warnings)
    return row


def parse_file(path_value: str, root_value: str) -> dict[str, Any]:
    path = Path(path_value)
    root = Path(root_value)
    row = empty_row(path, root)
    try:
        raw = path.read_bytes()
        return parse_html(path, root, raw)
    except Exception as error:  # Keep one output row even for a broken input file.
        try:
            row["source_size_bytes"] = path.stat().st_size
        except OSError:
            pass
        row["page_status"] = "parse_error"
        row["parse_error"] = normalize_text(f"{type(error).__name__}: {error}")
        return row


def iter_rows(
    paths: list[Path], root: Path, workers: int
) -> Iterator[dict[str, Any]]:
    parser = partial(parse_file, root_value=str(root))
    path_values = [str(path) for path in paths]
    if workers == 1:
        yield from map(parser, path_values)
        return
    try:
        executor = ProcessPoolExecutor(max_workers=workers)
    except (PermissionError, NotImplementedError):
        print(
            "Process workers are unavailable; falling back to threads",
            file=sys.stderr,
            flush=True,
        )
        with ThreadPoolExecutor(max_workers=workers) as thread_executor:
            yield from thread_executor.map(parser, path_values)
        return
    with executor:
        yield from executor.map(parser, path_values, chunksize=64)


def truncate_incomplete_row(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        return
    with path.open("r+b") as file:
        file.seek(-1, os.SEEK_END)
        if file.read(1) == b"\n":
            return
        position = file.tell() - 1
        while position > 0:
            position -= 1
            file.seek(position)
            if file.read(1) == b"\n":
                file.truncate(position + 1)
                return
        file.truncate(0)


def resume_state(
    part_path: Path, paths: list[Path], root: Path
) -> tuple[int, Counter[str]]:
    truncate_incomplete_row(part_path)
    if not part_path.exists() or part_path.stat().st_size == 0:
        return 0, Counter()
    last_row: dict[str, str] | None = None
    counts: Counter[str] = Counter()
    with part_path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, delimiter="\t")
        if reader.fieldnames != COLUMNS:
            raise ValueError(f"Cannot resume {part_path}: TSV schema has changed")
        count = 0
        for last_row in reader:
            count += 1
            counts[last_row.get("page_status", "")] += 1
    if count > len(paths):
        raise ValueError(f"Cannot resume {part_path}: it contains too many rows")
    if count and last_row:
        expected = paths[count - 1].relative_to(root).as_posix()
        if last_row.get("source_file") != expected:
            raise ValueError(
                f"Cannot resume {part_path}: input file order changed at row {count}"
            )
    return count, counts


def export_tsv(
    input_dir: Path,
    output_path: Path,
    *,
    workers: int,
    limit: int = 0,
    resume: bool = False,
    progress_every: int = 1000,
) -> Counter[str]:
    input_dir = input_dir.resolve()
    output_path = output_path.resolve()
    paths = sorted(input_dir.rglob("*.html"), key=lambda path: path.relative_to(input_dir).as_posix())
    if limit:
        paths = paths[:limit]
    if not paths:
        raise ValueError(f"No HTML files found in {input_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = Path(str(output_path) + ".part")
    if resume:
        offset, counts = resume_state(part_path, paths, input_dir)
    else:
        offset, counts = 0, Counter()
    mode = "a" if offset else "w"
    started = time.monotonic()

    with part_path.open(mode, encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=COLUMNS,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        if offset == 0:
            writer.writeheader()
        for index, row in enumerate(
            iter_rows(paths[offset:], input_dir, workers), start=offset + 1
        ):
            writer.writerow({key: normalize_text(value) for key, value in row.items()})
            counts[row["page_status"]] += 1
            if progress_every and (index % progress_every == 0 or index == len(paths)):
                file.flush()
                elapsed = max(time.monotonic() - started, 0.001)
                completed = index - offset
                print(
                    f"Processed {index}/{len(paths)} files "
                    f"({completed / elapsed:.1f} files/s)",
                    file=sys.stderr,
                    flush=True,
                )

    os.replace(part_path, output_path)
    return counts


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract saved Shikimori anime pages into one TSV row per HTML file."
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        type=Path,
        default=PROJECT_DIR / "pages",
        help="directory containing saved HTML pages (default: project pages/)",
    )
    parser.add_argument(
        "output",
        nargs="?",
        type=Path,
        default=PROJECT_DIR / "shikimori.tsv",
        help="output TSV path (default: project shikimori.tsv)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="parser processes (threads are a fallback); 0 chooses automatically (default: 0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="process only the first N files; useful for validation (default: all)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="continue a compatible OUTPUT.part left by an interrupted run",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="print and flush progress every N rows; 0 disables it (default: 1000)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    if args.workers < 0:
        raise SystemExit("--workers must be non-negative")
    if args.limit < 0:
        raise SystemExit("--limit must be non-negative")
    if args.progress_every < 0:
        raise SystemExit("--progress-every must be non-negative")
    workers = args.workers or min(8, os.cpu_count() or 1)
    counts = export_tsv(
        args.input_dir,
        args.output,
        workers=workers,
        limit=args.limit,
        resume=args.resume,
        progress_every=args.progress_every,
    )
    summary = ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
    print(f"Wrote {args.output}: {summary}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
