import gzip
import io
import json
import re
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

import download_shikimori_graphql as downloader


ANIME_SPEC = downloader.EntitySpec(
    "animes",
    "animes",
    (
        ("core", "  id\n  name"),
        ("stats", "  id\n  score"),
        ("roles", "  id\n  roles"),
    ),
    2,
)

CHARACTER_SPEC = downloader.EntitySpec(
    "characters",
    "characters",
    (("core", "  id\n  name"),),
    2,
)

TOPIC_ANIME_SPEC = downloader.EntitySpec(
    "animes",
    "animes",
    (
        ("core", "  id\n  name"),
        ("topic", downloader.ANIME_TOPIC_FIELDS),
    ),
    2,
)

PERSON_NULL_SPEC = downloader.EntitySpec(
    "people",
    "people",
    (("core", "  id\n  name\n  createdAt"),),
    3,
)


class FakeGraphQLClient:
    endpoint = "https://example.test/api/graphql"

    def __init__(
        self,
        pages,
        *,
        cache_fingerprint="test-context",
        fail_on_calls=(),
        reverse_supplements=False,
    ):
        self.pages = pages
        self.cache_fingerprint = cache_fingerprint
        self.fail_on_calls = set(fail_on_calls)
        self.reverse_supplements = reverse_supplements
        self.calls = []

    def request(self, query):
        self.calls.append(query)
        if len(self.calls) in self.fail_on_calls:
            raise downloader.DownloadError("injected interruption")

        operation = re.search(r"query\s+(\w+)", query).group(1)
        root = "animes" if " animes(" in query else "characters"
        ids_match = re.search(r"ids:\s*(\"(?:[^\"\\]|\\.)*\")", query)
        if ids_match:
            ids = json.loads(ids_match.group(1)).split(",")
        else:
            page = int(re.search(r"page:\s*(\d+)", query).group(1))
            ids = list(self.pages.get(page, []))

        if self.reverse_supplements and not operation.endswith("Core"):
            ids = list(reversed(ids))

        records = []
        for record_id in ids:
            record = {"id": str(record_id)}
            if operation == "DownloadAnimeChronology":
                record["chronology"] = [{"id": f"related-{record_id}"}]
            elif operation.endswith("Core"):
                record["name"] = f"name-{record_id}"
            elif operation.endswith("Stats"):
                record["score"] = f"score-{record_id}"
            elif operation.endswith("Roles"):
                record["roles"] = [f"role-{record_id}"]
            records.append(record)
        return {"data": {root: records}}


class TopicNullGraphQLClient:
    endpoint = "https://example.test/api/graphql"
    cache_fingerprint = "test-context"

    def __init__(self):
        self.calls = []

    def request(self, query):
        self.calls.append(query)
        if "DownloadAnimesCore" in query:
            return {
                "data": {
                    "animes": [
                        {"id": "1", "name": "valid"},
                        {"id": "2", "name": "broken"},
                    ]
                }
            }
        if "updatedAt" in query:
            payload = {
                "data": {
                    "animes": [
                        {
                            "id": "1",
                            "topic": {
                                "id": "101",
                                "body": "valid body",
                                "updatedAt": "2026-01-01T00:00:00Z",
                            },
                        },
                        {"id": "2", "topic": None},
                    ]
                },
                "errors": [
                    {
                        "message": (
                            "Cannot return null for non-nullable field "
                            "Topic.updatedAt"
                        )
                    }
                ],
            }
            raise downloader.PartialGraphQLDataError(
                "partial topic data",
                payload,
                {("Topic", "updatedAt")},
            )
        return {
            "data": {
                "animes": [
                    {"id": "1", "topic": {"id": "101", "body": "valid body"}},
                    {
                        "id": "2",
                        "topic": {"id": "102", "body": "recovered body"},
                    },
                ]
            }
        }


class PersonNullGraphQLClient:
    endpoint = "https://example.test/api/graphql"
    cache_fingerprint = "test-context"

    def __init__(self, *, fail_on_calls=()):
        self.calls = []
        self.fail_on_calls = set(fail_on_calls)

    @staticmethod
    def partial_error():
        payload = {
            "data": None,
            "errors": [
                {
                    "message": (
                        "Cannot return null for non-nullable field Person.createdAt"
                    )
                }
            ],
        }
        return downloader.PartialGraphQLDataError(
            "partial person data",
            payload,
            {("Person", "createdAt")},
        )

    def request(self, query):
        self.calls.append(query)
        if len(self.calls) in self.fail_on_calls:
            raise downloader.DownloadError("injected person recovery interruption")
        ids_match = re.search(r"ids:\s*(\[[^]]*\])", query)
        if ids_match:
            ids = json.loads(ids_match.group(1))
            if "2" in ids:
                raise self.partial_error()
            return {
                "data": {
                    "people": [
                        {"id": record_id, "createdAt": f"date-{record_id}"}
                        for record_id in ids
                    ]
                }
            }
        if "createdAt" in query:
            raise self.partial_error()
        return {
            "data": {
                "people": [
                    {"id": "1", "name": "one"},
                    {"id": "2", "name": "two"},
                    {"id": "3", "name": "three"},
                ]
            }
        }


class FakeHttpResponse:
    status_code = 200
    headers = {}
    text = ""

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload

    def raise_for_status(self):
        return None


class DownloadShikimoriGraphqlTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def download(
        self,
        client,
        spec,
        *,
        page_size=2,
        max_pages=0,
        build_jsonl_output=False,
        include_chronology=False,
    ):
        with redirect_stderr(io.StringIO()):
            return downloader.download_entity(
                client,
                spec,
                self.root,
                page_size=page_size,
                max_pages=max_pages,
                refresh=False,
                build_jsonl_output=build_jsonl_output,
                include_chronology=include_chronology,
            )

    def write_cached_page(
        self,
        spec,
        page,
        records,
        *,
        page_size=2,
        context="test-context",
    ):
        digest = downloader.page_cache_hash(
            spec,
            page,
            page_size,
            cache_context=context,
            include_chronology=False,
        )
        path = downloader.page_path(self.root, spec, page)
        downloader.write_gzip_json_atomic(
            path,
            {
                "_meta": {"query_sha256": digest},
                "records": records,
            },
        )
        return path, digest

    def test_merge_parts_by_id_and_preserve_core_order(self):
        responses = {
            "core": {
                "data": {
                    "animes": [
                        {"id": "1", "name": "one"},
                        {"id": "2", "name": "two"},
                    ]
                }
            },
            "stats": {
                "data": {
                    "animes": [
                        {"id": "2", "score": 8},
                        {"id": "1", "score": 9},
                    ]
                }
            },
            "roles": {
                "data": {
                    "animes": [
                        {"id": "1", "roles": ["main"]},
                        {"id": "2", "roles": ["supporting"]},
                    ]
                }
            },
        }

        records = downloader.merge_response_parts(responses, ANIME_SPEC)

        self.assertEqual([record["id"] for record in records], ["1", "2"])
        self.assertEqual(records[0]["score"], 9)
        self.assertEqual(records[1]["roles"], ["supporting"])

    def test_merge_rejects_mismatched_ids(self):
        responses = {
            "core": {"data": {"animes": [{"id": "1"}]}},
            "stats": {"data": {"animes": [{"id": "2"}]}},
            "roles": {"data": {"animes": [{"id": "1"}]}},
        }

        with self.assertRaisesRegex(ValueError, "different record IDs"):
            downloader.merge_response_parts(responses, ANIME_SPEC)

    def test_page_cache_is_invalidated_by_query_and_context(self):
        path, digest = self.write_cached_page(
            CHARACTER_SPEC, 1, [{"id": "1", "name": "one"}]
        )
        changed_spec = downloader.EntitySpec(
            "characters",
            "characters",
            (("core", "  id\n  name\n  updatedAt"),),
            2,
        )

        self.assertEqual(
            downloader.cached_records(path, CHARACTER_SPEC, digest)[0]["id"], "1"
        )
        with redirect_stderr(io.StringIO()):
            wrong_context = downloader.page_cache_hash(
                CHARACTER_SPEC,
                1,
                2,
                cache_context="another-context",
                include_chronology=False,
            )
            changed_query = downloader.page_cache_hash(
                changed_spec,
                1,
                2,
                cache_context="test-context",
                include_chronology=False,
            )
            self.assertIsNone(
                downloader.cached_records(path, CHARACTER_SPEC, wrong_context)
            )
            self.assertIsNone(
                downloader.cached_records(path, changed_spec, changed_query)
            )

    def test_interrupted_page_reuses_parts_and_cleans_them_after_commit(self):
        first_client = FakeGraphQLClient({1: ["1", "2"]}, fail_on_calls={3})
        with self.assertRaisesRegex(downloader.DownloadError, "interruption"):
            self.download(first_client, ANIME_SPEC, max_pages=1)

        core_path = downloader.page_part_path(self.root, ANIME_SPEC, 1, "core")
        stats_path = downloader.page_part_path(self.root, ANIME_SPEC, 1, "stats")
        roles_path = downloader.page_part_path(self.root, ANIME_SPEC, 1, "roles")
        self.assertTrue(core_path.exists())
        self.assertTrue(stats_path.exists())
        self.assertFalse(roles_path.exists())
        self.assertFalse(downloader.page_path(self.root, ANIME_SPEC, 1).exists())

        resumed_client = FakeGraphQLClient({1: ["1", "2"]})
        manifest = self.download(resumed_client, ANIME_SPEC, max_pages=1)

        self.assertEqual(len(resumed_client.calls), 1)
        self.assertIn("DownloadAnimesRoles", resumed_client.calls[0])
        self.assertEqual(manifest["record_count"], 2)
        self.assertFalse(core_path.exists())
        self.assertFalse(stats_path.exists())
        self.assertFalse(roles_path.exists())
        cached = downloader.read_gzip_json(
            downloader.page_path(self.root, ANIME_SPEC, 1)
        )
        self.assertEqual([row["id"] for row in cached["records"]], ["1", "2"])

    def test_recovers_topic_when_api_violates_updated_at_contract(self):
        client = TopicNullGraphQLClient()

        manifest = self.download(client, TOPIC_ANIME_SPEC, max_pages=1)

        self.assertEqual(len(client.calls), 3)
        self.assertIn("updatedAt", client.calls[1])
        self.assertNotIn("updatedAt", client.calls[2])
        self.assertEqual(manifest["record_count"], 2)
        page = downloader.read_gzip_json(
            downloader.page_path(self.root, TOPIC_ANIME_SPEC, 1)
        )
        records = page["records"]
        self.assertEqual(records[0]["topic"]["updatedAt"], "2026-01-01T00:00:00Z")
        self.assertEqual(records[1]["topic"]["updatedAt"], None)
        self.assertEqual(records[1]["topic"]["body"], "recovered body")

    def test_topic_fallback_keeps_outer_character_updated_at(self):
        fallback_topic_fields = downloader._topic_fields_without({"updatedAt"})
        fallback_selection = downloader.CHARACTER_FIELDS.replace(
            downloader.TOPIC_FIELDS, fallback_topic_fields
        )
        query = downloader._query_with_selection(
            downloader.ENTITY_SPECS["characters"],
            "core",
            fallback_selection,
            page=3,
            page_size=50,
            record_ids=None,
        )

        self.assertIn("characters(page: 3, limit: 50)", query)
        self.assertEqual(query.count("updatedAt"), 1)

    def test_recovers_root_person_created_at_per_id(self):
        client = PersonNullGraphQLClient()

        manifest = self.download(
            client,
            PERSON_NULL_SPEC,
            page_size=3,
            max_pages=1,
        )

        self.assertEqual(manifest["record_count"], 3)
        self.assertEqual(len(client.calls), 5)
        page = downloader.read_gzip_json(
            downloader.page_path(self.root, PERSON_NULL_SPEC, 1)
        )
        self.assertEqual(
            [record["createdAt"] for record in page["records"]],
            ["date-1", None, "date-3"],
        )
        self.assertFalse(
            downloader.person_recovery_path(self.root, 1, "createdAt").exists()
        )

    def test_interrupted_person_recovery_resumes_per_id_cache(self):
        first_client = PersonNullGraphQLClient(fail_on_calls={4})

        with self.assertRaisesRegex(downloader.DownloadError, "interruption"):
            self.download(
                first_client,
                PERSON_NULL_SPEC,
                page_size=3,
                max_pages=1,
            )

        recovery_path = downloader.person_recovery_path(
            self.root, 1, "createdAt"
        )
        self.assertTrue(recovery_path.exists())
        cached = downloader.read_gzip_json(recovery_path)
        self.assertEqual(cached["values"], {"1": "date-1"})

        resumed_client = PersonNullGraphQLClient()
        self.download(
            resumed_client,
            PERSON_NULL_SPEC,
            page_size=3,
            max_pages=1,
        )

        self.assertEqual(len(resumed_client.calls), 4)
        self.assertFalse(recovery_path.exists())

    def test_null_contract_with_data_null_is_not_retried(self):
        client = downloader.GraphQLClient(
            "https://example.test/api/graphql",
            timeout=1,
            retries=5,
            requests_per_second=1,
            requests_per_minute=80,
            user_agent="test",
        )
        payload = {
            "data": None,
            "errors": [
                {
                    "message": (
                        "Cannot return null for non-nullable field Person.createdAt"
                    )
                }
            ],
        }
        calls = []
        client.rate_limiter.wait = lambda: None

        def post(*_args, **_kwargs):
            calls.append(1)
            return FakeHttpResponse(payload)

        client.session.post = post

        with self.assertRaises(downloader.PartialGraphQLDataError):
            client.request("query { people { id createdAt } }")
        self.assertEqual(len(calls), 1)

    def test_only_pure_non_null_errors_are_classified_for_recovery(self):
        null_error = {
            "message": "Cannot return null for non-nullable field Topic.updatedAt"
        }

        self.assertEqual(
            downloader._non_null_contract_violations([null_error]),
            {("Topic", "updatedAt")},
        )
        self.assertEqual(
            downloader._non_null_contract_violations(
                [null_error, {"message": "Internal server error"}]
            ),
            set(),
        )

    def test_stale_part_ids_trigger_one_full_page_refetch(self):
        current_ids = ["3", "4"]
        stats_selection = dict(ANIME_SPEC.selections)["stats"]
        stats_query = ANIME_SPEC.query_for_ids("stats", stats_selection, current_ids)
        stats_digest = downloader.query_set_hash(
            [("stats", stats_query)], context="test-context"
        )
        stats_path = downloader.page_part_path(self.root, ANIME_SPEC, 1, "stats")
        downloader.write_response_part(
            stats_path,
            {
                "data": {
                    "animes": [
                        {"id": "1", "score": "stale-1"},
                        {"id": "2", "score": "stale-2"},
                    ]
                }
            },
            spec=ANIME_SPEC,
            part="stats",
            page=1,
            page_size=2,
            endpoint=FakeGraphQLClient.endpoint,
            digest=stats_digest,
        )

        client = FakeGraphQLClient({1: current_ids})
        self.download(client, ANIME_SPEC, max_pages=1)

        self.assertEqual(len(client.calls), 5)
        self.assertEqual(
            sum("DownloadAnimesCore" in query for query in client.calls), 2
        )
        self.assertEqual(
            sum("DownloadAnimesStats" in query for query in client.calls), 1
        )
        self.assertEqual(
            sum("DownloadAnimesRoles" in query for query in client.calls), 2
        )
        page = downloader.read_gzip_json(
            downloader.page_path(self.root, ANIME_SPEC, 1)
        )
        self.assertEqual([record["id"] for record in page["records"]], current_ids)
        self.assertEqual(page["records"][0]["score"], "score-3")
        self.assertFalse(stats_path.exists())

    def test_interrupted_per_id_chronology_resumes_into_aggregate(self):
        first_client = FakeGraphQLClient({1: ["1", "2"]}, fail_on_calls={5})
        with self.assertRaisesRegex(downloader.DownloadError, "interruption"):
            self.download(
                first_client,
                ANIME_SPEC,
                max_pages=1,
                include_chronology=True,
            )

        chronology_1 = downloader.chronology_part_path(self.root, "1")
        chronology_2 = downloader.chronology_part_path(self.root, "2")
        self.assertTrue(chronology_1.exists())
        self.assertFalse(chronology_2.exists())
        self.assertFalse(downloader.page_path(self.root, ANIME_SPEC, 1).exists())

        resumed_client = FakeGraphQLClient({1: ["1", "2"]})
        manifest = self.download(
            resumed_client,
            ANIME_SPEC,
            max_pages=1,
            include_chronology=True,
        )

        self.assertEqual(len(resumed_client.calls), 1)
        self.assertIn("DownloadAnimeChronology", resumed_client.calls[0])
        self.assertIn('ids: "2"', resumed_client.calls[0])
        self.assertTrue(manifest["chronology_included"])
        page = downloader.read_gzip_json(
            downloader.page_path(self.root, ANIME_SPEC, 1)
        )
        self.assertEqual(
            [record["chronology"] for record in page["records"]],
            [[{"id": "related-1"}], [{"id": "related-2"}]],
        )
        self.assertFalse(chronology_1.exists())
        self.assertFalse(chronology_2.exists())
        for part, _ in ANIME_SPEC.selections:
            self.assertFalse(
                downloader.page_part_path(self.root, ANIME_SPEC, 1, part).exists()
            )

    def test_exact_multiple_downloads_an_empty_sentinel_page(self):
        client = FakeGraphQLClient(
            {
                1: ["1", "2"],
                2: ["3", "4"],
                3: [],
            }
        )

        manifest = self.download(client, CHARACTER_SPEC)

        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["last_page"], 3)
        self.assertEqual(manifest["record_count"], 4)
        self.assertEqual(len(client.calls), 3)
        empty_page = downloader.read_gzip_json(
            downloader.page_path(self.root, CHARACTER_SPEC, 3)
        )
        self.assertEqual(empty_page["records"], [])

    def test_nonempty_short_page_does_not_end_pagination(self):
        client = FakeGraphQLClient({1: ["1"], 2: []})

        manifest = self.download(client, CHARACTER_SPEC, page_size=2)

        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["last_page"], 2)
        self.assertEqual(len(client.calls), 2)

    def test_jsonl_rejects_duplicate_ids_across_pages(self):
        self.write_cached_page(CHARACTER_SPEC, 1, [{"id": "1"}])
        self.write_cached_page(CHARACTER_SPEC, 2, [{"id": "1"}])

        with self.assertRaisesRegex(downloader.DownloadError, "duplicate id 1"):
            downloader.build_jsonl(
                CHARACTER_SPEC,
                self.root,
                2,
                2,
                cache_context="test-context",
                include_chronology=False,
            )

    def test_failed_jsonl_build_keeps_previous_output_and_removes_temp(self):
        self.write_cached_page(CHARACTER_SPEC, 1, [{"id": "1"}])
        self.write_cached_page(CHARACTER_SPEC, 2, [{"id": "1"}])
        destination = self.root / "characters" / "characters.jsonl.gz"
        destination.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(destination, "wt", encoding="utf-8") as output:
            output.write('{"id":"old"}\n')

        with self.assertRaises(downloader.DownloadError):
            downloader.build_jsonl(
                CHARACTER_SPEC,
                self.root,
                2,
                2,
                cache_context="test-context",
                include_chronology=False,
            )

        with gzip.open(destination, "rt", encoding="utf-8") as output:
            self.assertEqual(output.read(), '{"id":"old"}\n')
        self.assertEqual(
            list(destination.parent.glob(f".{destination.name}.*.tmp")), []
        )

    def test_schema_coverage_rejects_new_and_removed_fields(self):
        types = {
            type_name: {
                "fields": [
                    {"name": field_name} for field_name in sorted(field_names)
                ]
            }
            for type_name, field_names in downloader.COMPLETE_TYPE_FIELDS.items()
        }
        downloader.validate_schema_coverage(types)
        types["Anime"]["fields"].append({"name": "futureField"})
        types["Anime"]["fields"] = [
            field for field in types["Anime"]["fields"] if field["name"] != "score"
        ]

        with self.assertRaises(downloader.DownloadError) as raised:
            downloader.validate_schema_coverage(types)

        self.assertIn("downloader is missing futureField", str(raised.exception))
        self.assertIn("fields absent from API: score", str(raised.exception))

    def test_complete_type_field_counts_match_live_schema(self):
        self.assertEqual(
            {
                type_name: len(downloader.COMPLETE_TYPE_FIELDS[type_name])
                for type_name in ("Anime", "Character", "Person")
            },
            {"Anime": 46, "Character": 17, "Person": 17},
        )


if __name__ == "__main__":
    unittest.main()
