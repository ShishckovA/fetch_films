#!/usr/bin/env python3
"""Download Anime, Character, and Person records with resumable page caching.

The downloader requests every field currently exposed by the three root
GraphQL types. Character/person role targets are stored as IDs and resolved
against the separately downloaded complete Character/Person datasets. Nested
Anime/Manga objects in relations are kept as summaries rather than recursively
requesting their own relations and roles, which would form cycles and exceed
Shikimori's maximum query depth.

Data is first cached page-by-page as gzip-compressed JSON. Each page is written
atomically and tagged with its exact query hash. A restart reuses only pages
created by the current query. After every run, cached pages are materialized as
one JSON object per line in ``<entity>/<entity>.jsonl.gz``.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import random
import re
import sys
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_ENDPOINT = "https://shikimori.io/api/graphql"
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "graphql"

DATE_FIELDS = """
  date
  day
  month
  year
"""

POSTER_FIELDS = """
  id
  originalUrl
  mainUrl
  main2xUrl
  mainAltUrl
  mainAlt2xUrl
  previewUrl
  preview2xUrl
  previewAltUrl
  previewAlt2xUrl
  miniUrl
  mini2xUrl
  miniAltUrl
  miniAlt2xUrl
"""

TOPIC_FIELDS = """
  id
  type
  title
  body
  htmlBody
  tags
  commentsCount
  createdAt
  updatedAt
  url
"""

TOPIC_FIELD_NAMES = (
    "id",
    "type",
    "title",
    "body",
    "htmlBody",
    "tags",
    "commentsCount",
    "createdAt",
    "updatedAt",
    "url",
)

PERSON_RECOVERABLE_ROOT_FIELDS = {
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
}

CHARACTER_FIELDS = f"""
  id
  malId
  name
  russian
  japanese
  synonyms
  description
  descriptionHtml
  descriptionSource
  isAnime
  isManga
  isRanobe
  createdAt
  updatedAt
  url
  poster {{
    {POSTER_FIELDS}
  }}
  topic {{
    {TOPIC_FIELDS}
  }}
"""

PERSON_FIELDS = f"""
  id
  malId
  name
  russian
  japanese
  synonyms
  website
  isMangaka
  isProducer
  isSeyu
  createdAt
  updatedAt
  url
  birthOn {{
    {DATE_FIELDS}
  }}
  deceasedOn {{
    {DATE_FIELDS}
  }}
  poster {{
    {POSTER_FIELDS}
  }}
  topic {{
    {TOPIC_FIELDS}
  }}
"""

ANIME_SUMMARY_FIELDS = f"""
  id
  malId
  name
  russian
  english
  japanese
  synonyms
  licenseNameRu
  licensors
  url
  kind
  status
  episodes
  episodesAired
  duration
  score
  rating
  origin
  franchise
  isCensored
  season
  createdAt
  updatedAt
  nextEpisodeAt
  opengraphImageUrl
  airedOn {{
    {DATE_FIELDS}
  }}
  releasedOn {{
    {DATE_FIELDS}
  }}
  poster {{
    {POSTER_FIELDS}
  }}
  genres {{
    id
    name
    russian
    kind
    entryType
  }}
  studios {{
    id
    name
    imageUrl
  }}
"""

MANGA_SUMMARY_FIELDS = f"""
  id
  malId
  name
  russian
  english
  japanese
  synonyms
  licenseNameRu
  licensors
  url
  kind
  status
  chapters
  volumes
  score
  franchise
  isCensored
  createdAt
  updatedAt
  opengraphImageUrl
  airedOn {{
    {DATE_FIELDS}
  }}
  releasedOn {{
    {DATE_FIELDS}
  }}
  poster {{
    {POSTER_FIELDS}
  }}
  genres {{
    id
    name
    russian
    kind
    entryType
  }}
  publishers {{
    id
    name
  }}
"""

RELATED_FIELDS = f"""
  id
  relationKind
  relationEn
  relationRu
  relationText
  anime {{
    {ANIME_SUMMARY_FIELDS}
  }}
  manga {{
    {MANGA_SUMMARY_FIELDS}
  }}
"""

ANIME_CORE_FIELDS = f"""
  {ANIME_SUMMARY_FIELDS}
  description
  descriptionHtml
  descriptionSource
  fandubbers
  fansubbers
  externalLinks {{
    id
    kind
    url
    createdAt
    updatedAt
  }}
"""

ANIME_STATS_FIELDS = f"""
  id
  scoresStats {{
    score
    count
  }}
  statusesStats {{
    status
    count
  }}
"""

ANIME_MEDIA_FIELDS = f"""
  id
  screenshots {{
    id
    originalUrl
    x166Url
    x332Url
  }}
  videos {{
    id
    url
    imageUrl
    playerUrl
    name
    kind
  }}
"""

ANIME_TOPIC_FIELDS = f"""
  id
  topic {{
    {TOPIC_FIELDS}
  }}
"""

ANIME_USER_RATE_FIELDS = f"""
  id
  userRate {{
    id
    status
    score
    episodes
    chapters
    volumes
    rewatches
    text
    createdAt
    updatedAt
  }}
"""

ANIME_USER_RATE_ANIME_FIELDS = f"""
  id
  userRate {{
    anime {{ id }}
  }}
"""

ANIME_USER_RATE_MANGA_FIELDS = f"""
  id
  userRate {{
    manga {{ id }}
  }}
"""

ANIME_CHARACTER_ROLES_FIELDS = f"""
  id
  characterRoles {{
    id
    rolesEn
    rolesRu
    character {{
      id
    }}
  }}
"""

ANIME_PERSON_ROLES_FIELDS = f"""
  id
  personRoles {{
    id
    rolesEn
    rolesRu
    person {{
      id
    }}
  }}
"""

ANIME_RELATED_FIELDS = f"""
  id
  related {{
    {RELATED_FIELDS}
  }}
"""

ANIME_CHRONOLOGY_FIELDS = f"""
  id
  chronology {{
    {ANIME_SUMMARY_FIELDS}
  }}
"""

SCHEMA_TYPE_NAMES = [
    "Query",
    "Anime",
    "AnimeKindEnum",
    "AnimeOriginEnum",
    "AnimeRatingEnum",
    "AnimeStatusEnum",
    "Character",
    "CharacterRole",
    "ExternalLink",
    "ExternalLinkKindEnum",
    "Genre",
    "GenreEntryTypeEnum",
    "GenreKindEnum",
    "IncompleteDate",
    "Manga",
    "MangaKindEnum",
    "MangaStatusEnum",
    "Person",
    "PersonRole",
    "Poster",
    "Publisher",
    "Related",
    "RelationKindEnum",
    "ScoreStat",
    "Screenshot",
    "StatusStat",
    "Studio",
    "Topic",
    "User",
    "UserRate",
    "UserRateStatusEnum",
    "Video",
    "VideoKindEnum",
]

# These types are selected exhaustively. Reference targets inside recursive
# types are intentionally represented by IDs or non-recursive summaries.
COMPLETE_TYPE_FIELDS = {
    "Anime": {
        "airedOn",
        "characterRoles",
        "chronology",
        "createdAt",
        "description",
        "descriptionHtml",
        "descriptionSource",
        "duration",
        "english",
        "episodes",
        "episodesAired",
        "externalLinks",
        "fandubbers",
        "fansubbers",
        "franchise",
        "genres",
        "id",
        "isCensored",
        "japanese",
        "kind",
        "licenseNameRu",
        "licensors",
        "malId",
        "name",
        "nextEpisodeAt",
        "opengraphImageUrl",
        "origin",
        "personRoles",
        "poster",
        "rating",
        "related",
        "releasedOn",
        "russian",
        "score",
        "scoresStats",
        "screenshots",
        "season",
        "status",
        "statusesStats",
        "studios",
        "synonyms",
        "topic",
        "updatedAt",
        "url",
        "userRate",
        "videos",
    },
    "Character": {
        "createdAt",
        "description",
        "descriptionHtml",
        "descriptionSource",
        "id",
        "isAnime",
        "isManga",
        "isRanobe",
        "japanese",
        "malId",
        "name",
        "poster",
        "russian",
        "synonyms",
        "topic",
        "updatedAt",
        "url",
    },
    "Person": {
        "birthOn",
        "createdAt",
        "deceasedOn",
        "id",
        "isMangaka",
        "isProducer",
        "isSeyu",
        "japanese",
        "malId",
        "name",
        "poster",
        "russian",
        "synonyms",
        "topic",
        "updatedAt",
        "url",
        "website",
    },
    "CharacterRole": {"character", "id", "rolesEn", "rolesRu"},
    "ExternalLink": {"createdAt", "id", "kind", "updatedAt", "url"},
    "Genre": {"entryType", "id", "kind", "name", "russian"},
    "IncompleteDate": {"date", "day", "month", "year"},
    "PersonRole": {"id", "person", "rolesEn", "rolesRu"},
    "Poster": {
        "id",
        "main2xUrl",
        "mainAlt2xUrl",
        "mainAltUrl",
        "mainUrl",
        "mini2xUrl",
        "miniAlt2xUrl",
        "miniAltUrl",
        "miniUrl",
        "originalUrl",
        "preview2xUrl",
        "previewAlt2xUrl",
        "previewAltUrl",
        "previewUrl",
    },
    "Publisher": {"id", "name"},
    "Related": {
        "anime",
        "id",
        "manga",
        "relationEn",
        "relationKind",
        "relationRu",
        "relationText",
    },
    "ScoreStat": {"count", "score"},
    "Screenshot": {"id", "originalUrl", "x166Url", "x332Url"},
    "StatusStat": {"count", "status"},
    "Studio": {"id", "imageUrl", "name"},
    "Topic": {
        "body",
        "commentsCount",
        "createdAt",
        "htmlBody",
        "id",
        "tags",
        "title",
        "type",
        "updatedAt",
        "url",
    },
    "UserRate": {
        "anime",
        "chapters",
        "createdAt",
        "episodes",
        "id",
        "manga",
        "rewatches",
        "score",
        "status",
        "text",
        "updatedAt",
        "volumes",
    },
    "Video": {"id", "imageUrl", "kind", "name", "playerUrl", "url"},
}


@dataclass(frozen=True)
class EntitySpec:
    name: str
    root_field: str
    selections: tuple[tuple[str, str], ...]
    default_page_size: int

    def queries(self, page: int, page_size: int) -> list[tuple[str, str]]:
        arguments = f"page: {page}, limit: {page_size}"
        if self.name == "animes":
            arguments += ", order: id, censored: false"
        queries = []
        for part, selection in self.selections:
            operation = "".join(word.title() for word in f"{self.name}_{part}".split("_"))
            query = (
                f"query Download{operation} {{\n  {self.root_field}({arguments}) "
                f"{{\n{selection}\n  }}\n}}"
            )
            queries.append((part, query))
        return queries

    def query_for_ids(
        self, part: str, selection: str, record_ids: list[str]
    ) -> str:
        if self.name != "animes":
            raise ValueError("ID-based supplemental queries are only used for anime")
        ids = ",".join(record_ids)
        arguments = (
            f"ids: {json.dumps(ids)}, limit: {len(record_ids)}, "
            "order: id, censored: false"
        )
        operation = "".join(word.title() for word in f"{self.name}_{part}".split("_"))
        return (
            f"query Download{operation} {{\n  {self.root_field}({arguments}) "
            f"{{\n{selection}\n  }}\n}}"
        )

    @property
    def selection_signature(self) -> str:
        return "\n".join(f"{part}\n{selection}" for part, selection in self.selections)


ENTITY_SPECS = {
    "animes": EntitySpec(
        "animes",
        "animes",
        (
            ("core", ANIME_CORE_FIELDS),
            ("stats", ANIME_STATS_FIELDS),
            ("media", ANIME_MEDIA_FIELDS),
            ("topic", ANIME_TOPIC_FIELDS),
            ("user_rate", ANIME_USER_RATE_FIELDS),
            ("user_rate_anime", ANIME_USER_RATE_ANIME_FIELDS),
            ("user_rate_manga", ANIME_USER_RATE_MANGA_FIELDS),
            ("related", ANIME_RELATED_FIELDS),
            ("character_roles", ANIME_CHARACTER_ROLES_FIELDS),
            ("person_roles", ANIME_PERSON_ROLES_FIELDS),
        ),
        50,
    ),
    "characters": EntitySpec(
        "characters", "characters", (("core", CHARACTER_FIELDS),), 50
    ),
    "people": EntitySpec("people", "people", (("core", PERSON_FIELDS),), 50),
}


class DownloadError(RuntimeError):
    pass


class PartialGraphQLDataError(DownloadError):
    def __init__(
        self,
        message: str,
        payload: dict[str, Any],
        violations: set[tuple[str, str]],
    ) -> None:
        super().__init__(message)
        self.payload = payload
        self.violations = violations


class TransientGraphQLError(RuntimeError):
    pass


class RateLimiter:
    def __init__(self, requests_per_second: float, requests_per_minute: int) -> None:
        self.interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self.requests_per_minute = requests_per_minute
        self.next_request_at = 0.0
        self.recent_requests: deque[float] = deque()

    def wait(self) -> None:
        while True:
            now = time.monotonic()
            while self.recent_requests and self.recent_requests[0] <= now - 60.0:
                self.recent_requests.popleft()
            allowed_at = self.next_request_at
            if len(self.recent_requests) >= self.requests_per_minute:
                allowed_at = max(allowed_at, self.recent_requests[0] + 60.0)
            if now >= allowed_at:
                self.recent_requests.append(now)
                self.next_request_at = now + self.interval
                return
            time.sleep(allowed_at - now)


class GraphQLClient:
    def __init__(
        self,
        endpoint: str,
        *,
        timeout: float,
        retries: int,
        requests_per_second: float,
        requests_per_minute: int,
        user_agent: str,
        token: str | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.timeout = timeout
        self.retries = retries
        self.rate_limiter = RateLimiter(requests_per_second, requests_per_minute)
        self.session = requests.Session()
        # Corporate proxy variables break access to Shikimori in this environment.
        self.session.trust_env = False
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": user_agent,
            }
        )
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        token_fingerprint = hashlib.sha256((token or "anonymous").encode()).hexdigest()
        self.cache_fingerprint = hashlib.sha256(
            f"{endpoint}\n{token_fingerprint}".encode("utf-8")
        ).hexdigest()

    def request(self, query: str) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            self.rate_limiter.wait()
            try:
                response = self.session.post(
                    self.endpoint,
                    json={"query": query},
                    timeout=(15, self.timeout),
                )
                if response.status_code in {408, 425, 429} or response.status_code >= 500:
                    retry_after = response.headers.get("Retry-After")
                    delay = _retry_delay(attempt, retry_after)
                    raise requests.HTTPError(
                        f"HTTP {response.status_code}; retry in {delay:.1f}s",
                        response=response,
                    )
                if response.status_code >= 400:
                    raise DownloadError(
                        f"HTTP {response.status_code}: {response.text[:500]}"
                    )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise TransientGraphQLError(
                        f"GraphQL returned {type(payload).__name__}, expected an object"
                    )
                if payload.get("errors"):
                    messages = "; ".join(
                        str(error.get("message", error))
                        if isinstance(error, dict)
                        else str(error)
                        for error in payload["errors"]
                    )
                    violations = _non_null_contract_violations(payload["errors"])
                    if violations:
                        raise PartialGraphQLDataError(
                            f"GraphQL returned partial data: {messages}",
                            payload,
                            violations,
                        )
                    if _is_deterministic_graphql_error(messages):
                        raise DownloadError(f"GraphQL errors: {messages}")
                    raise TransientGraphQLError(f"GraphQL errors: {messages}")
                if not isinstance(payload.get("data"), dict):
                    raise DownloadError("GraphQL response has no data object")
                return payload
            except DownloadError:
                # Query/schema errors are deterministic and should be fixed, not retried.
                raise
            except (requests.RequestException, TransientGraphQLError, ValueError) as error:
                last_error = error
                if attempt >= self.retries:
                    break
                retry_after = (
                    error.response.headers.get("Retry-After")
                    if isinstance(error, requests.HTTPError) and error.response is not None
                    else None
                )
                delay = _retry_delay(attempt, retry_after)
                print(
                    f"Request failed ({error}); retry {attempt + 1}/{self.retries} "
                    f"in {delay:.1f}s",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(delay)
        raise DownloadError(f"GraphQL request failed after retries: {last_error}")


NON_NULL_CONTRACT_ERROR = re.compile(
    r"Cannot return null for non-nullable field "
    r"(?P<type>[A-Za-z_][A-Za-z0-9_]*)\."
    r"(?P<field>[A-Za-z_][A-Za-z0-9_]*)"
)


def _non_null_contract_violations(errors: Any) -> set[tuple[str, str]]:
    if not isinstance(errors, list) or not errors:
        return set()
    violations = set()
    for error in errors:
        message = error.get("message") if isinstance(error, dict) else str(error)
        if not isinstance(message, str):
            return set()
        match = NON_NULL_CONTRACT_ERROR.search(message)
        if not match:
            return set()
        violations.add((match.group("type"), match.group("field")))
    return violations


def _is_deterministic_graphql_error(message: str) -> bool:
    markers = (
        "doesn't exist",
        "Cannot query field",
        "Unknown argument",
        "Expected type",
        "Parse error",
        "exceeds max complexity",
        "exceeds max depth",
        "Variable $",
    )
    return any(marker in message for marker in markers)


def _retry_delay(attempt: int, retry_after: str | None) -> float:
    if retry_after:
        try:
            return max(float(retry_after), 0.0)
        except ValueError:
            pass
    return min(60.0, 2.0**attempt) + random.uniform(0.0, 0.5)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def query_hash(query: str) -> str:
    return hashlib.sha256(query.encode("utf-8")).hexdigest()


def query_set_hash(
    queries: Iterable[tuple[str, str]], *, context: str = ""
) -> str:
    serialized = json.dumps(
        {"context": context, "queries": list(queries)},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return query_hash(serialized)


def page_path(output_dir: Path, spec: EntitySpec, page: int) -> Path:
    return output_dir / spec.name / "pages" / f"page_{page:06d}.json.gz"


def page_part_path(
    output_dir: Path, spec: EntitySpec, page: int, part: str
) -> Path:
    return output_dir / spec.name / "page_parts" / f"page_{page:06d}.{part}.json.gz"


def chronology_part_path(output_dir: Path, anime_id: str) -> Path:
    digest = hashlib.sha256(anime_id.encode("utf-8")).hexdigest()[:20]
    return output_dir / "animes" / "chronology_parts" / f"id_{digest}.json.gz"


def person_recovery_path(output_dir: Path, page: int, field: str) -> Path:
    return (
        output_dir
        / "people"
        / "recovery_parts"
        / f"page_{page:06d}.Person.{field}.json.gz"
    )


def chronology_query(anime_id: str) -> str:
    return (
        "query DownloadAnimeChronology {\n"
        f"  animes(ids: {json.dumps(anime_id)}, limit: 1, order: id, "
        "censored: false) {\n"
        f"{ANIME_CHRONOLOGY_FIELDS}\n"
        "  }\n"
        "}"
    )


def page_cache_hash(
    spec: EntitySpec,
    page: int,
    page_size: int,
    *,
    cache_context: str,
    include_chronology: bool,
) -> str:
    logical_queries = spec.queries(page, page_size)
    if spec.name == "animes" and include_chronology:
        logical_queries.append(("chronology_per_anime", ANIME_CHRONOLOGY_FIELDS))
    return query_set_hash(logical_queries, context=cache_context)


def read_gzip_json(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"Expected an object in {path}")
    return value


def write_gzip_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as file:
            json.dump(value, file, ensure_ascii=False, separators=(",", ":"))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(value, file, ensure_ascii=False, indent=2, sort_keys=True)
            file.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def records_from_page(
    document: dict[str, Any], spec: EntitySpec, expected_hash: str
) -> list[dict[str, Any]]:
    metadata = document.get("_meta")
    if not isinstance(metadata, dict) or metadata.get("query_sha256") != expected_hash:
        raise ValueError("Cached page was produced by a different query")
    records = document.get("records")
    if records is not None:
        if not isinstance(records, list) or not all(
            isinstance(record, dict) for record in records
        ):
            raise ValueError("Cached page has invalid records")
        ids = [str(record.get("id", "")) for record in records]
        if not all(ids) or len(ids) != len(set(ids)):
            raise ValueError("Cached page has missing or duplicate record IDs")
        return records
    # Backward compatibility for caches made by early versions of this script.
    responses = document.get("responses")
    if not isinstance(responses, dict):
        raise ValueError("Cached page has neither records nor GraphQL response parts")
    return merge_response_parts(responses, spec)


def response_records(
    response: dict[str, Any], spec: EntitySpec, part: str
) -> list[dict[str, Any]]:
    if response.get("errors"):
        raise ValueError(f"GraphQL response part {part} contains errors")
    data = response.get("data")
    records = data.get(spec.root_field) if isinstance(data, dict) else None
    if not isinstance(records, list) or not all(
        isinstance(record, dict) for record in records
    ):
        raise ValueError(f"Response part {part} has no {spec.root_field} list")
    ids = [str(record.get("id", "")) for record in records]
    if not all(ids):
        raise ValueError(f"Response part {part} contains a record without id")
    if len(ids) != len(set(ids)):
        raise ValueError(f"Response part {part} contains duplicate IDs")
    return records


def merge_record_fields(
    target: dict[str, Any], source: dict[str, Any], *, part: str, record_id: str
) -> None:
    for key, value in source.items():
        if key not in target:
            target[key] = value
            continue
        existing = target[key]
        if existing == value:
            continue
        if isinstance(existing, dict) and isinstance(value, dict):
            merge_record_fields(
                existing,
                value,
                part=part,
                record_id=record_id,
            )
            continue
        raise ValueError(
            f"Response part {part} disagrees on {key} for id {record_id}"
        )


def merge_response_parts(
    responses: dict[str, Any], spec: EntitySpec
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] | None = None
    expected_ids: list[str] | None = None
    expected_parts = [part for part, _ in spec.selections]
    if set(responses) != set(expected_parts):
        raise ValueError(
            f"Response parts mismatch: got {sorted(responses)}, expected {expected_parts}"
        )
    for part in expected_parts:
        response = responses[part]
        if not isinstance(response, dict):
            raise ValueError(f"Invalid GraphQL response part: {part}")
        records = response_records(response, spec, part)
        ids = [str(record.get("id", "")) for record in records]
        if expected_ids is None:
            expected_ids = ids
            merged = [dict(record) for record in records]
            continue
        if set(ids) != set(expected_ids):
            raise ValueError(f"Response part {part} returned different record IDs")
        assert merged is not None
        records_by_id = {str(record["id"]): record for record in records}
        for target in merged:
            source = records_by_id[str(target["id"])]
            merge_record_fields(
                target, source, part=part, record_id=str(source["id"])
            )
    return merged or []


def cached_records(
    path: Path, spec: EntitySpec, expected_hash: str
) -> list[dict[str, Any]] | None:
    if not path.exists():
        return None
    try:
        return records_from_page(read_gzip_json(path), spec, expected_hash)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Ignoring invalid cache {path}: {error}", file=sys.stderr, flush=True)
        return None


def cached_response_part(path: Path, expected_hash: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        document = read_gzip_json(path)
        metadata = document.get("_meta")
        response = document.get("response")
        if not isinstance(metadata, dict) or metadata.get("query_sha256") != expected_hash:
            raise ValueError("part query hash mismatch")
        if not isinstance(response, dict) or response.get("errors"):
            raise ValueError("invalid GraphQL response")
        return response
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Ignoring invalid part cache {path}: {error}", file=sys.stderr, flush=True)
        return None


def write_response_part(
    path: Path,
    response: dict[str, Any],
    *,
    spec: EntitySpec,
    part: str,
    page: int,
    page_size: int,
    endpoint: str,
    digest: str,
) -> None:
    write_gzip_json_atomic(
        path,
        {
            "_meta": {
                "entity": spec.name,
                "part": part,
                "page": page,
                "page_size": page_size,
                "fetched_at": utc_now(),
                "endpoint": endpoint,
                "query_sha256": digest,
            },
            "response": response,
        },
    )


def _topic_fields_without(excluded_fields: set[str]) -> str:
    remaining = [name for name in TOPIC_FIELD_NAMES if name not in excluded_fields]
    if not remaining:
        return "\n  __typename\n"
    return "\n" + "\n".join(f"  {name}" for name in remaining) + "\n"


def _query_with_selection(
    spec: EntitySpec,
    part: str,
    selection: str,
    *,
    page: int,
    page_size: int,
    record_ids: list[str] | None,
) -> str:
    if spec.name == "animes" and part != "core":
        if record_ids is None:
            raise DownloadError("Anime fallback query has no core IDs")
        return spec.query_for_ids(part, selection, record_ids)
    fallback_spec = EntitySpec(
        spec.name,
        spec.root_field,
        ((part, selection),),
        spec.default_page_size,
    )
    return fallback_spec.queries(page, page_size)[0][1]


def _response_records_ignoring_errors(
    response: dict[str, Any], spec: EntitySpec, part: str
) -> list[dict[str, Any]]:
    sanitized = dict(response)
    sanitized.pop("errors", None)
    return response_records(sanitized, spec, part)


def _merge_recovered_topic_responses(
    partial_responses: list[dict[str, Any]],
    fallback_response: dict[str, Any],
    spec: EntitySpec,
    part: str,
    excluded_fields: set[str],
) -> dict[str, Any]:
    fallback_records = response_records(fallback_response, spec, part)
    merged_records = []
    for record in fallback_records:
        copied = dict(record)
        if isinstance(copied.get("topic"), dict):
            copied["topic"] = dict(copied["topic"])
        merged_records.append(copied)
    records_by_id = {str(record["id"]): record for record in merged_records}
    expected_ids = set(records_by_id)

    recovered_errors = []
    for partial_response in partial_responses:
        partial_records = _response_records_ignoring_errors(
            partial_response, spec, part
        )
        partial_ids = {str(record["id"]) for record in partial_records}
        if partial_ids != expected_ids:
            raise DownloadError(
                f"Recovered part {part} returned different record IDs"
            )
        errors = partial_response.get("errors")
        if isinstance(errors, list):
            recovered_errors.extend(errors)
        for source in partial_records:
            record_id = str(source["id"])
            target = records_by_id[record_id]
            source_topic = source.get("topic")
            if not isinstance(source_topic, dict):
                continue
            target_topic = target.get("topic")
            if target_topic is not None and not isinstance(target_topic, dict):
                raise DownloadError(f"Record {record_id} has an invalid topic")
            if isinstance(target_topic, dict):
                for field in excluded_fields:
                    if field in source_topic:
                        target_topic[field] = source_topic[field]

    for record in merged_records:
        topic = record.get("topic")
        if isinstance(topic, dict):
            for field in excluded_fields:
                topic.setdefault(field, None)

    return {
        "data": {spec.root_field: merged_records},
        "_recovered_graphql_errors": recovered_errors,
    }


def recover_topic_contract_violation(
    client: GraphQLClient,
    spec: EntitySpec,
    part: str,
    selection: str,
    error: PartialGraphQLDataError,
    *,
    page: int,
    page_size: int,
    record_ids: list[str] | None,
) -> dict[str, Any]:
    if TOPIC_FIELDS not in selection:
        raise error
    unsupported = {
        violation for violation in error.violations if violation[0] != "Topic"
    }
    excluded_fields = {
        field for type_name, field in error.violations if type_name == "Topic"
    }
    if unsupported or not excluded_fields or not excluded_fields <= set(TOPIC_FIELD_NAMES):
        raise error

    partial_responses = [error.payload]
    while True:
        fallback_topic_fields = _topic_fields_without(excluded_fields)
        fallback_selection = selection.replace(TOPIC_FIELDS, fallback_topic_fields)
        if fallback_selection == selection:
            raise error
        print(
            f"{spec.name}: page={page} part={part} recovering invalid nulls in "
            f"Topic.{','.join(sorted(excluded_fields))}",
            file=sys.stderr,
            flush=True,
        )
        fallback_query = _query_with_selection(
            spec,
            part,
            fallback_selection,
            page=page,
            page_size=page_size,
            record_ids=record_ids,
        )
        try:
            fallback_response = client.request(fallback_query)
        except PartialGraphQLDataError as fallback_error:
            fallback_unsupported = {
                violation
                for violation in fallback_error.violations
                if violation[0] != "Topic"
            }
            new_fields = {
                field
                for type_name, field in fallback_error.violations
                if type_name == "Topic" and field not in excluded_fields
            }
            if (
                fallback_unsupported
                or not new_fields
                or not new_fields <= set(TOPIC_FIELD_NAMES)
            ):
                raise fallback_error
            partial_responses.append(fallback_error.payload)
            excluded_fields.update(new_fields)
            continue
        return _merge_recovered_topic_responses(
            partial_responses,
            fallback_response,
            spec,
            part,
            excluded_fields,
        )


def _selection_without_top_level_fields(
    selection: str, excluded_fields: set[str]
) -> str:
    depth = 0
    found = set()
    output = []
    for line in selection.splitlines(keepends=True):
        field_name = line.strip()
        if depth == 0 and field_name in excluded_fields:
            found.add(field_name)
            continue
        output.append(line)
        depth += line.count("{") - line.count("}")
        if depth < 0:
            raise DownloadError("Invalid GraphQL selection nesting")
    if depth != 0 or found != excluded_fields:
        missing = ", ".join(sorted(excluded_fields - found))
        raise DownloadError(f"Could not omit root fields from selection: {missing}")
    return "".join(output)


def _person_field_query(field: str, record_ids: list[str]) -> str:
    if field not in PERSON_RECOVERABLE_ROOT_FIELDS:
        raise DownloadError(f"Person.{field} cannot be recovered as a scalar field")
    ids = json.dumps(record_ids, ensure_ascii=False, separators=(",", ":"))
    operation = re.sub(r"[^A-Za-z0-9_]", "", field.title())
    return (
        f"query RecoverPerson{operation} {{\n"
        f"  people(ids: {ids}, limit: {len(record_ids)}) {{\n"
        "    id\n"
        f"    {field}\n"
        "  }\n"
        "}"
    )


def _recover_person_field_values(
    client: GraphQLClient,
    spec: EntitySpec,
    field: str,
    record_ids: list[str],
    *,
    page: int,
    output_dir: Path,
) -> tuple[dict[str, Any], list[Any], Path]:
    cache_path = person_recovery_path(output_dir, page, field)
    cache_digest = query_set_hash(
        [(f"Person.{field}", _person_field_query(field, record_ids))],
        context=client.cache_fingerprint,
    )
    expected_ids = set(record_ids)
    values: dict[str, Any] = {}
    recovered_errors: list[Any] = []
    if cache_path.exists():
        try:
            cached = read_gzip_json(cache_path)
            metadata = cached.get("_meta")
            cached_values = cached.get("values")
            if (
                not isinstance(metadata, dict)
                or metadata.get("query_sha256") != cache_digest
                or not isinstance(cached_values, dict)
                or not set(cached_values) <= expected_ids
            ):
                raise ValueError("person recovery cache mismatch")
            values.update(cached_values)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            print(
                f"Ignoring invalid recovery cache {cache_path}: {error}",
                file=sys.stderr,
                flush=True,
            )

    def persist() -> None:
        write_gzip_json_atomic(
            cache_path,
            {
                "_meta": {
                    "entity": spec.name,
                    "field": field,
                    "page": page,
                    "updated_at": utc_now(),
                    "endpoint": client.endpoint,
                    "query_sha256": cache_digest,
                },
                "values": values,
            },
        )

    for record_id in record_ids:
        if record_id in values:
            continue
        query = _person_field_query(field, [record_id])
        try:
            response = client.request(query)
        except PartialGraphQLDataError as error:
            if error.violations != {("Person", field)}:
                raise error
            errors = error.payload.get("errors")
            if isinstance(errors, list):
                recovered_errors.extend(errors)
            values[record_id] = None
            print(
                f"people: page={page} id={record_id} storing Person.{field}=null",
                file=sys.stderr,
                flush=True,
            )
            persist()
            continue

        records = response_records(response, spec, f"recover_{field}")
        if len(records) != 1 or str(records[0]["id"]) != record_id:
            raise DownloadError(
                f"Person.{field} recovery returned a different record ID"
            )
        if field not in records[0]:
            raise DownloadError(f"Person.{field} recovery omitted the selected field")
        values[record_id] = records[0][field]
        persist()

    return values, recovered_errors, cache_path


def recover_person_contract_violation(
    client: GraphQLClient,
    spec: EntitySpec,
    part: str,
    selection: str,
    error: PartialGraphQLDataError,
    *,
    page: int,
    page_size: int,
    output_dir: Path,
) -> dict[str, Any]:
    if spec.name != "people" or part != "core":
        raise error
    unsupported = {
        violation for violation in error.violations if violation[0] != "Person"
    }
    excluded_fields = {
        field for type_name, field in error.violations if type_name == "Person"
    }
    if (
        unsupported
        or not excluded_fields
        or not excluded_fields <= PERSON_RECOVERABLE_ROOT_FIELDS
    ):
        raise error

    recovered_errors = list(error.payload.get("errors") or [])
    while True:
        fallback_selection = _selection_without_top_level_fields(
            selection, excluded_fields
        )
        print(
            f"people: page={page} recovering invalid nulls in "
            + ",".join(f"Person.{field}" for field in sorted(excluded_fields)),
            file=sys.stderr,
            flush=True,
        )
        fallback_query = _query_with_selection(
            spec,
            part,
            fallback_selection,
            page=page,
            page_size=page_size,
            record_ids=None,
        )
        try:
            fallback_response = client.request(fallback_query)
        except PartialGraphQLDataError as fallback_error:
            person_fields = {
                field
                for type_name, field in fallback_error.violations
                if type_name == "Person"
            }
            other_violations = {
                violation
                for violation in fallback_error.violations
                if violation[0] != "Person"
            }
            new_fields = person_fields - excluded_fields
            if (
                not other_violations
                and new_fields
                and new_fields <= PERSON_RECOVERABLE_ROOT_FIELDS
            ):
                errors = fallback_error.payload.get("errors")
                if isinstance(errors, list):
                    recovered_errors.extend(errors)
                excluded_fields.update(new_fields)
                continue
            if all(
                type_name == "Topic"
                for type_name, _field in fallback_error.violations
            ):
                fallback_response = recover_topic_contract_violation(
                    client,
                    spec,
                    part,
                    fallback_selection,
                    fallback_error,
                    page=page,
                    page_size=page_size,
                    record_ids=None,
                )
            else:
                raise fallback_error
        break

    records = [dict(record) for record in response_records(fallback_response, spec, part)]
    record_ids = [str(record["id"]) for record in records]
    recovery_paths = []
    for field in sorted(excluded_fields):
        values, errors, cache_path = _recover_person_field_values(
            client,
            spec,
            field,
            record_ids,
            page=page,
            output_dir=output_dir,
        )
        recovery_paths.append(str(cache_path))
        recovered_errors.extend(errors)
        for record in records:
            record[field] = values[str(record["id"])]

    nested_errors = fallback_response.get("_recovered_graphql_errors")
    if isinstance(nested_errors, list):
        recovered_errors.extend(nested_errors)
    return {
        "data": {spec.root_field: records},
        "_recovered_graphql_errors": recovered_errors,
        "_transient_cache_paths": recovery_paths,
    }


def recover_contract_violation(
    client: GraphQLClient,
    spec: EntitySpec,
    part: str,
    selection: str,
    error: PartialGraphQLDataError,
    *,
    page: int,
    page_size: int,
    record_ids: list[str] | None,
    output_dir: Path,
) -> dict[str, Any]:
    if all(type_name == "Topic" for type_name, _field in error.violations):
        return recover_topic_contract_violation(
            client,
            spec,
            part,
            selection,
            error,
            page=page,
            page_size=page_size,
            record_ids=record_ids,
        )
    if all(type_name == "Person" for type_name, _field in error.violations):
        return recover_person_contract_violation(
            client,
            spec,
            part,
            selection,
            error,
            page=page,
            page_size=page_size,
            output_dir=output_dir,
        )
    raise error


def _fetch_page_parts(
    client: GraphQLClient,
    spec: EntitySpec,
    output_dir: Path,
    *,
    page: int,
    page_size: int,
    force_network: bool,
) -> tuple[dict[str, dict[str, Any]], list[Path], int, int]:
    responses: dict[str, dict[str, Any]] = {}
    transient_paths: list[Path] = []
    fetched_parts = 0
    cached_parts = 0
    core_ids: list[str] | None = None
    page_queries = dict(spec.queries(page, page_size))

    for index, (part, selection) in enumerate(spec.selections):
        if index == 0 and part != "core":
            raise DownloadError(f"{spec.name}: first query part must be core")
        if spec.name == "animes" and part != "core":
            if core_ids is None:
                raise DownloadError("Anime supplemental query has no core IDs")
            if not core_ids:
                responses[part] = {"data": {spec.root_field: []}}
                continue
            query = spec.query_for_ids(part, selection, core_ids)
        else:
            query = page_queries[part]

        part_digest = query_set_hash(
            [(part, query)], context=client.cache_fingerprint
        )
        path = page_part_path(output_dir, spec, page, part)
        response = None if force_network else cached_response_part(path, part_digest)
        recovery_cache_paths: list[Path] = []
        from_cache = response is not None
        if response is not None:
            try:
                response_records(response, spec, part)
            except ValueError as error:
                print(
                    f"Ignoring invalid part cache {path}: {error}",
                    file=sys.stderr,
                    flush=True,
                )
                response = None
                from_cache = False
        if response is None:
            print(
                f"{spec.name}: page={page} fetching={part}",
                file=sys.stderr,
                flush=True,
            )
            try:
                response = client.request(query)
            except PartialGraphQLDataError as error:
                response = recover_contract_violation(
                    client,
                    spec,
                    part,
                    selection,
                    error,
                    page=page,
                    page_size=page_size,
                    record_ids=core_ids,
                    output_dir=output_dir,
                )
                recovery_paths = response.pop("_transient_cache_paths", [])
                if isinstance(recovery_paths, list):
                    recovery_cache_paths = [Path(path) for path in recovery_paths]
                    transient_paths.extend(recovery_cache_paths)
            try:
                response_records(response, spec, part)
            except ValueError as error:
                raise DownloadError(f"Page {page}, part {part}: {error}") from error
            write_response_part(
                path,
                response,
                spec=spec,
                part=part,
                page=page,
                page_size=page_size,
                endpoint=client.endpoint,
                digest=part_digest,
            )
            # The canonical part now contains all recovered values atomically.
            cleanup_transient_parts(recovery_cache_paths)
            fetched_parts += 1
        elif from_cache:
            cached_parts += 1
        transient_paths.append(path)
        responses[part] = response

        if part == "core":
            core_ids = [
                str(record["id"]) for record in response_records(response, spec, part)
            ]

    return responses, transient_paths, fetched_parts, cached_parts


def _chronology_from_response(
    response: dict[str, Any], spec: EntitySpec, anime_id: str
) -> list[dict[str, Any]]:
    records = response_records(response, spec, "chronology")
    if len(records) != 1 or str(records[0]["id"]) != anime_id:
        raise ValueError(f"Chronology response does not contain anime {anime_id}")
    chronology = records[0].get("chronology")
    if not isinstance(chronology, list) or not all(
        isinstance(record, dict) for record in chronology
    ):
        raise ValueError(f"Anime {anime_id} has invalid chronology")
    return chronology


def attach_anime_chronology(
    client: GraphQLClient,
    spec: EntitySpec,
    output_dir: Path,
    records: list[dict[str, Any]],
    *,
    page: int,
    force_network: bool,
) -> tuple[list[Path], int, int]:
    transient_paths: list[Path] = []
    fetched_parts = 0
    cached_parts = 0
    for index, record in enumerate(records, 1):
        anime_id = str(record["id"])
        query = chronology_query(anime_id)
        digest = query_set_hash(
            [("chronology", query)], context=client.cache_fingerprint
        )
        path = chronology_part_path(output_dir, anime_id)
        response = None if force_network else cached_response_part(path, digest)
        chronology = None
        if response is not None:
            try:
                chronology = _chronology_from_response(response, spec, anime_id)
            except ValueError as error:
                print(
                    f"Ignoring invalid chronology cache {path}: {error}",
                    file=sys.stderr,
                    flush=True,
                )
                response = None
        if response is None:
            print(
                f"animes: page={page} chronology={index}/{len(records)} id={anime_id}",
                file=sys.stderr,
                flush=True,
            )
            response = client.request(query)
            try:
                chronology = _chronology_from_response(response, spec, anime_id)
            except ValueError as error:
                raise DownloadError(
                    f"Page {page}, chronology for anime {anime_id}: {error}"
                ) from error
            write_response_part(
                path,
                response,
                spec=spec,
                part="chronology",
                page=page,
                page_size=1,
                endpoint=client.endpoint,
                digest=digest,
            )
            fetched_parts += 1
        else:
            cached_parts += 1
        assert chronology is not None
        record["chronology"] = chronology
        transient_paths.append(path)
    return transient_paths, fetched_parts, cached_parts


def cleanup_transient_parts(paths: Iterable[Path]) -> None:
    for path in set(paths):
        try:
            path.unlink(missing_ok=True)
        except OSError as error:
            print(f"Could not remove transient cache {path}: {error}", file=sys.stderr)


def download_entity(
    client: GraphQLClient,
    spec: EntitySpec,
    output_dir: Path,
    *,
    page_size: int,
    max_pages: int,
    refresh: bool,
    build_jsonl_output: bool,
    include_chronology: bool = True,
) -> dict[str, Any]:
    page = 1
    total_records = 0
    complete = False
    last_page = 0
    network_pages = 0
    cached_pages = 0
    network_requests = 0
    cached_request_parts = 0

    while not max_pages or page <= max_pages:
        digest = page_cache_hash(
            spec,
            page,
            page_size,
            cache_context=client.cache_fingerprint,
            include_chronology=include_chronology,
        )
        path = page_path(output_dir, spec, page)
        records = None if refresh else cached_records(path, spec, digest)
        source = "cache"
        if records is None:
            responses, transient_paths, fetched_parts, part_cache_hits = (
                _fetch_page_parts(
                    client,
                    spec,
                    output_dir,
                    page=page,
                    page_size=page_size,
                    force_network=refresh,
                )
            )
            try:
                records = merge_response_parts(responses, spec)
            except ValueError as error:
                if not refresh and part_cache_hits:
                    print(
                        f"{spec.name}: page={page} cached parts disagree; "
                        "refetching the page once",
                        file=sys.stderr,
                        flush=True,
                    )
                    (
                        retry_responses,
                        retry_paths,
                        retry_fetched,
                        retry_cache_hits,
                    ) = (
                        _fetch_page_parts(
                            client,
                            spec,
                            output_dir,
                            page=page,
                            page_size=page_size,
                            force_network=True,
                        )
                    )
                    responses = retry_responses
                    transient_paths.extend(retry_paths)
                    fetched_parts += retry_fetched
                    part_cache_hits += retry_cache_hits
                    try:
                        records = merge_response_parts(responses, spec)
                    except ValueError as retry_error:
                        raise DownloadError(f"Page {page}: {retry_error}") from retry_error
                else:
                    raise DownloadError(f"Page {page}: {error}") from error

            if spec.name == "animes" and include_chronology and records:
                chronology_paths, chronology_fetched, chronology_cached = (
                    attach_anime_chronology(
                        client,
                        spec,
                        output_dir,
                        records,
                        page=page,
                        force_network=refresh,
                    )
                )
                transient_paths.extend(chronology_paths)
                fetched_parts += chronology_fetched
                part_cache_hits += chronology_cached
            document = {
                "_meta": {
                    "entity": spec.name,
                    "page": page,
                    "page_size": page_size,
                    "record_count": len(records),
                    "fetched_at": utc_now(),
                    "endpoint": client.endpoint,
                    "query_sha256": digest,
                    "include_chronology": bool(
                        spec.name == "animes" and include_chronology
                    ),
                },
                "records": records,
            }
            write_gzip_json_atomic(path, document)
            transient_paths.extend(
                (output_dir / spec.name / "page_parts").glob(
                    f"page_{page:06d}.*.json.gz"
                )
            )
            cleanup_transient_parts(transient_paths)
            source = f"network_parts={fetched_parts}" if fetched_parts else "part_cache"
            network_pages += int(fetched_parts > 0)
            network_requests += fetched_parts
            cached_request_parts += part_cache_hits
        else:
            cached_pages += 1

        last_page = page
        total_records += len(records)
        print(
            f"{spec.name}: page={page} rows={len(records)} total={total_records} "
            f"source={source}",
            file=sys.stderr,
            flush=True,
        )
        # The API exposes no total/pageInfo; an empty page is the only safe EOF.
        if not records:
            complete = True
            break
        page += 1

    manifest = {
        "entity": spec.name,
        "complete": complete,
        "page_size": page_size,
        "last_page": last_page,
        "record_count": total_records,
        "network_pages": network_pages,
        "network_requests": network_requests,
        "cached_pages": cached_pages,
        "cached_request_parts": cached_request_parts,
        "updated_at": utc_now(),
        "endpoint": client.endpoint,
        "selection_sha256": query_hash(spec.selection_signature),
    }
    if spec.name == "animes":
        manifest["chronology_included"] = include_chronology
    if build_jsonl_output and last_page:
        jsonl_count = build_jsonl(
            spec,
            output_dir,
            last_page,
            page_size,
            cache_context=client.cache_fingerprint,
            include_chronology=include_chronology,
        )
        if jsonl_count != total_records:
            raise DownloadError(
                f"{spec.name}: cache total {total_records} != JSONL total {jsonl_count}"
            )
        manifest["jsonl"] = str(output_dir / spec.name / f"{spec.name}.jsonl.gz")
    write_json_atomic(output_dir / spec.name / "manifest.json", manifest)
    return manifest


def build_jsonl(
    spec: EntitySpec,
    output_dir: Path,
    last_page: int,
    page_size: int,
    *,
    cache_context: str,
    include_chronology: bool = True,
) -> int:
    destination = output_dir / spec.name / f"{spec.name}.jsonl.gz"
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent, prefix=f".{destination.name}.", suffix=".tmp"
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    seen_ids: set[str] = set()
    count = 0
    try:
        with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as output:
            for page in range(1, last_page + 1):
                path = page_path(output_dir, spec, page)
                records = records_from_page(
                    read_gzip_json(path),
                    spec,
                    page_cache_hash(
                        spec,
                        page,
                        page_size,
                        cache_context=cache_context,
                        include_chronology=include_chronology,
                    ),
                )
                for record in records:
                    record_id = str(record.get("id", ""))
                    if not record_id:
                        raise DownloadError(
                            f"{spec.name} page {page}: record without id"
                        )
                    if record_id in seen_ids:
                        raise DownloadError(
                            f"{spec.name} page {page}: duplicate id {record_id}"
                        )
                    seen_ids.add(record_id)
                    output.write(
                        json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                    )
                    output.write("\n")
                    count += 1
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return count


def schema_query(type_name: str) -> str:
    # One type per request keeps introspection below the API complexity limit.
    return f"""query Schema{type_name} {{
  type: __type(name: \"{type_name}\") {{
    name
    kind
    fields(includeDeprecated: true) {{
      name
      isDeprecated
      deprecationReason
      args {{ name defaultValue }}
      type {{ kind name ofType {{ kind name }} }}
    }}
    enumValues(includeDeprecated: true) {{
      name
      isDeprecated
      deprecationReason
    }}
    inputFields {{ name type {{ kind name ofType {{ kind name }} }} }}
  }}
}}"""


def validate_schema_coverage(types: dict[str, Any]) -> None:
    problems = []
    for type_name, selected_fields in COMPLETE_TYPE_FIELDS.items():
        type_data = types.get(type_name)
        fields = type_data.get("fields") if isinstance(type_data, dict) else None
        if not isinstance(fields, list):
            problems.append(f"{type_name}: absent from introspection")
            continue
        schema_fields = {
            str(field["name"])
            for field in fields
            if isinstance(field, dict) and field.get("name")
        }
        missing = schema_fields - selected_fields
        removed = selected_fields - schema_fields
        if missing:
            problems.append(
                f"{type_name}: downloader is missing {', '.join(sorted(missing))}"
            )
        if removed:
            problems.append(
                f"{type_name}: fields absent from API: {', '.join(sorted(removed))}"
            )
    if problems:
        raise DownloadError(
            "GraphQL schema differs from the exhaustive selections:\n- "
            + "\n- ".join(problems)
            + "\nUpdate the query selections, or use --skip-schema only if this is expected."
        )


def save_schema_snapshot(
    client: GraphQLClient, output_dir: Path, *, refresh: bool
) -> Path:
    path = output_dir / "schema.json.gz"
    queries = [(type_name, schema_query(type_name)) for type_name in SCHEMA_TYPE_NAMES]
    digest = query_set_hash(queries, context=client.cache_fingerprint)
    if not refresh and path.exists():
        try:
            document = read_gzip_json(path)
            if document.get("_meta", {}).get("query_sha256") == digest:
                types = document.get("types")
                if not isinstance(types, dict):
                    raise ValueError("schema cache has no types")
                validate_schema_coverage(types)
                return path
        except (OSError, ValueError, json.JSONDecodeError):
            pass
    types = {}
    for index, (type_name, query) in enumerate(queries, 1):
        part_digest = query_set_hash(
            [(type_name, query)], context=client.cache_fingerprint
        )
        part_path = output_dir / "schema_types" / f"{type_name}.json.gz"
        type_data = None
        if not refresh and part_path.exists():
            try:
                cached = read_gzip_json(part_path)
                if cached.get("_meta", {}).get("query_sha256") == part_digest:
                    type_data = cached.get("type")
            except (OSError, ValueError, json.JSONDecodeError):
                pass
        if type_data is None:
            print(
                f"schema: {index}/{len(queries)} {type_name}",
                file=sys.stderr,
                flush=True,
            )
            response = client.request(query)
            type_data = response.get("data", {}).get("type")
            if not isinstance(type_data, dict):
                raise DownloadError(f"Schema introspection returned no {type_name} type")
            write_gzip_json_atomic(
                part_path,
                {
                    "_meta": {
                        "type": type_name,
                        "fetched_at": utc_now(),
                        "endpoint": client.endpoint,
                        "query_sha256": part_digest,
                    },
                    "type": type_data,
                },
            )
        types[type_name] = type_data
    validate_schema_coverage(types)
    document = {
        "_meta": {
            "fetched_at": utc_now(),
            "endpoint": client.endpoint,
            "query_sha256": digest,
            "types": SCHEMA_TYPE_NAMES,
        },
        "types": types,
    }
    write_gzip_json_atomic(path, document)
    return path


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--entities",
        nargs="+",
        choices=tuple(ENTITY_SPECS),
        default=list(ENTITY_SPECS),
        help="entity datasets to download (default: all)",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--anime-page-size", type=int, default=50)
    parser.add_argument("--character-page-size", type=int, default=50)
    parser.add_argument("--people-page-size", type=int, default=50)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="stop after this page number per entity; 0 means until exhaustion",
    )
    parser.add_argument("--requests-per-second", type=float, default=1.0)
    parser.add_argument("--requests-per-minute", type=int, default=80)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--token", default=os.environ.get("SHIKIMORI_TOKEN"))
    parser.add_argument(
        "--user-agent",
        default="shikimori-graphql-dump/1.0 (research dataset downloader)",
    )
    parser.add_argument(
        "--refresh", action="store_true", help="ignore compatible cached pages"
    )
    parser.add_argument("--skip-schema", action="store_true")
    parser.add_argument(
        "--skip-chronology",
        action="store_true",
        help=(
            "omit Anime.chronology; the API only allows this field with limit=1, "
            "so including it requires one extra request per anime"
        ),
    )
    parser.add_argument("--no-jsonl", action="store_true")
    parser.add_argument(
        "--print-query",
        choices=tuple(ENTITY_SPECS),
        help="print the page-1 query for an entity and exit",
    )
    return parser


def validate_arguments(args: argparse.Namespace) -> None:
    sizes = [args.anime_page_size, args.character_page_size, args.people_page_size]
    if any(size < 1 or size > 50 for size in sizes):
        raise SystemExit("page sizes must be between 1 and 50")
    if args.max_pages < 0:
        raise SystemExit("--max-pages must be non-negative")
    if args.requests_per_second <= 0 or args.requests_per_second > 5:
        raise SystemExit("--requests-per-second must be in (0, 5]")
    if args.requests_per_minute < 1 or args.requests_per_minute > 90:
        raise SystemExit("--requests-per-minute must be between 1 and 90")
    if args.timeout <= 0:
        raise SystemExit("--timeout must be positive")
    if args.retries < 0:
        raise SystemExit("--retries must be non-negative")


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    validate_arguments(args)
    if args.print_query:
        spec = ENTITY_SPECS[args.print_query]
        for part, query in spec.queries(1, spec.default_page_size):
            print(f"# --- {part} ---")
            print(query)
        if spec.name == "animes":
            print("# --- chronology (one request per anime) ---")
            print(chronology_query("ANIME_ID"))
        return 0

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    client = GraphQLClient(
        args.endpoint,
        timeout=args.timeout,
        retries=args.retries,
        requests_per_second=args.requests_per_second,
        requests_per_minute=args.requests_per_minute,
        user_agent=args.user_agent,
        token=args.token,
    )
    if not args.skip_schema:
        schema_path = save_schema_snapshot(client, output_dir, refresh=args.refresh)
        print(f"schema: {schema_path}", file=sys.stderr, flush=True)

    page_sizes = {
        "animes": args.anime_page_size,
        "characters": args.character_page_size,
        "people": args.people_page_size,
    }
    results = []
    for entity in args.entities:
        results.append(
            download_entity(
                client,
                ENTITY_SPECS[entity],
                output_dir,
                page_size=page_sizes[entity],
                max_pages=args.max_pages,
                refresh=args.refresh,
                build_jsonl_output=not args.no_jsonl,
                include_chronology=not args.skip_chronology,
            )
        )
    print(json.dumps(results, ensure_ascii=False, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
