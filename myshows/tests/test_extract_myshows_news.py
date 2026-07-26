import gzip
import hashlib
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import extract_myshows_news as extractor


def flatten_nuxt(root):
    """Build the subset of devalue's flattened format used in fixtures."""

    values = [None]

    def add(value):
        index = len(values)
        values.append(None)
        if isinstance(value, dict):
            values[index] = {key: add(child) for key, child in value.items()}
        elif isinstance(value, list):
            values[index] = [add(child) for child in value]
        else:
            values[index] = value
        return index

    values[0] = ["ShallowReactive", add(root)]
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def article_page(news_id=10, *, content=None, comments=None, comments_count=None):
    if content is None:
        content = (
            '<p>Полный <a href="/view/1/" title="Шоу">текст</a></p>'
            '<img src="/poster.jpg" alt="Постер" width="100">'
        )
    if comments is None:
        comments = [
            {
                "comment": {"id": 1, "comment": "root"},
                "comments": [
                    {
                        "comment": {"id": 2, "comment": "reply"},
                        "comments": [
                            {"comment": {"id": 3, "comment": "deep"}, "comments": []}
                        ],
                    }
                ],
            }
        ]
    if comments_count is None:
        comments_count = 3
    slug = "test-news"
    route = f"/news/{news_id}/{slug}/"
    news = {
        "id": news_id,
        "title": "Тестовая новость",
        "alias": slug,
        "foreword": "Краткое описание",
        "content": content,
        "publishedAt": "2026-01-02T03:04:05+0300",
        "images": ["abc.jpg"],
        "image": "https://media.myshows.me/news/normal/a/abc.jpg",
        "imageInfo": {"width": 1920, "height": 1080},
        "author": {"href": "/writer", "anchor": "writer", "targetBlank": False},
        "video": "<iframe src=\"/video\"></iframe>",
        "commentsTotal": comments_count,
        "commentsNew": 1,
        "mediaSource": "Кадр из сериала",
        "tags": [{"title": "Netflix", "alias": "netflix"}],
        "category": {"title": "Статьи", "alias": "articles"},
        "source": "myshows",
        "alt": "Обложка",
    }
    page_data = {
        "news": news,
        "similarNews": [{"id": 9}],
        "categories": [{"title": "Статьи", "alias": "articles"}],
        "comments": comments,
        "commentsMeta": {
            "count": comments_count,
            "newCount": 1,
            "hasSpoilers": True,
        },
        "commentsWithImages": [{"id": 2}],
        "readAlsoAside": [{"id": 8}],
        "readAlsoMain": [{"id": 7}],
        "emotions": [
            {"emotionId": 2, "title": "Лайк", "count": 4},
            {"emotionId": 3, "title": "Огонь", "count": 2},
            {"emotionId": 4, "title": "Дизлайк", "count": 1},
        ],
        "author": {"id": 55, "user": {"login": "writer"}},
        "authorNewsCounter": 12,
        "catalogLinks": [{"title": "Драмы", "url": "/search/drama/", "count": 5}],
    }
    payload = flatten_nuxt(
        {
            "data": {route: page_data},
            "state": {},
            "_errors": {},
            "serverRendered": True,
            "path": route,
        }
    )
    return f"""<!doctype html><html><head>
<title>SEO заголовок</title>
<meta name="description" content="SEO описание">
<meta property="og:title" content="OG заголовок">
<meta property="og:url" content="https://myshows.me{route}">
<meta property="og:image" content="https://media.myshows.me/fallback.jpg">
<link rel="canonical" href="https://myshows.me{route}">
<link rel="alternate" hreflang="en" href="https://en.myshows.me{route}">
<script type="application/ld+json">{json.dumps({
    "@context": "https://schema.org",
    "@type": "Article",
    "headline": "Тестовая новость",
    "datePublished": "2026-01-02T03:04:05+0300",
    "dateModified": "2026-01-03T03:04:05+0300",
    "author": {"@type": "Person", "name": "writer", "url": "/writer", "description": "Автор MyShows"},
}, ensure_ascii=False)}</script>
</head><body><div class="NewsDetails">
<h1 class="NewsDetails__title">Тестовая новость</h1>
<div class="NewsDetails__foreword">Краткое описание</div>
<div class="NewsDetails__author-description">Автор MyShows | 12 статей</div>
<div class="NewsDetails__poster"><img src="/rendered.jpg" alt="Rendered alt"></div>
<div class="NewsPoster__caption">Rendered caption</div>
</div><script type="application/json" id="__NUXT_DATA__">{payload}</script>
</body></html>"""


def not_found_page(news_id=20):
    payload = flatten_nuxt(
        {
            "error": {
                "statusCode": 404,
                "statusMessage": "Server Error",
                "message": "Not found",
            },
            "data": {},
            "_errors": {},
            "path": f"/news/{news_id}/",
        }
    )
    return f"""<html><head><title>Оппс… Заблудился?</title></head>
<body><script id="__NUXT_DATA__" type="application/json">{payload}</script></body></html>"""


class ExtractMyShowsNewsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.input = self.root / "news"
        self.output = self.root / "parsed"
        self.input.mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, news_id, text):
        path = self.input / f"{news_id}.html"
        path.write_text(text, encoding="utf-8")
        return path

    def parse_one(self, news_id):
        path = self.input / f"{news_id}.html"
        return extractor.parse_source(
            extractor.SourceInput(news_id, path), self.input.resolve()
        ).row

    def read_rows(self):
        with gzip.open(self.output / "news.jsonl.gz", "rt", encoding="utf-8") as file:
            return [json.loads(line) for line in file]

    def test_extracts_nuxt_article_and_all_structured_fields(self):
        self.write(10, article_page())

        row = self.parse_one(10)

        self.assertEqual(set(row), set(extractor.FIELDS))
        self.assertEqual(row["source_id"], 10)
        self.assertEqual(row["news_id"], 10)
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["slug"], "test-news")
        self.assertEqual(row["title"], "Тестовая новость")
        self.assertIn("Полный текст", row["content_text"])
        self.assertIn("<a href=", row["content_html"])
        self.assertEqual(row["modified_at"], "2026-01-03T03:04:05+0300")
        self.assertEqual(row["image_width"], 1920)
        self.assertEqual(row["author_id"], 55)
        self.assertEqual(row["author_articles_count"], 12)
        self.assertEqual(row["category_slug"], "articles")
        self.assertEqual(row["comments_loaded_count"], 3)
        self.assertEqual(row["comments_meta_count"], 3)
        self.assertTrue(row["comments_complete"])
        self.assertTrue(row["has_spoilers"])
        self.assertEqual(row["reaction_like_count"], 4)
        self.assertEqual(row["reaction_fire_count"], 2)
        self.assertEqual(row["reaction_dislike_count"], 1)
        self.assertEqual(row["reaction_total"], 7)
        self.assertEqual(row["content_links"][0]["url"], "https://myshows.me/view/1/")
        self.assertEqual(row["content_images"][0]["url"], "https://myshows.me/poster.jpg")
        self.assertEqual(row["similar_news"], [{"id": 9}])
        self.assertEqual(row["comments"][0]["comment"]["id"], 1)
        self.assertEqual(row["author"]["user"]["login"], "writer")
        self.assertEqual(row["seo_meta"][0]["key"], "description")
        self.assertEqual(row["hreflang_links"][0]["hreflang"], "en")
        self.assertEqual(row["json_ld"][0]["@type"], "Article")
        self.assertEqual(row["nuxt_news"]["id"], 10)

    def test_counts_nested_comment_ids_uniquely_and_reports_incomplete(self):
        comments = [
            {
                "comment": {"id": 1},
                "comments": [
                    {"comment": {"id": 2}, "comments": []},
                    {"comment": {"id": 2}, "comments": []},
                ],
            }
        ]
        self.write(10, article_page(comments=comments, comments_count=3))

        row = self.parse_one(10)

        self.assertEqual(row["comments_loaded_count"], 2)
        self.assertEqual(row["comments_meta_count"], 3)
        self.assertFalse(row["comments_complete"])

    def test_preserves_404_as_diagnostic_row(self):
        self.write(20, not_found_page())

        row = self.parse_one(20)

        self.assertEqual(row["source_id"], 20)
        self.assertIsNone(row["news_id"])
        self.assertEqual(row["status"], "not_found")
        self.assertEqual(row["error_status_code"], 404)
        self.assertEqual(row["error_message"], "Server Error: Not found")
        self.assertEqual(row["page_title"], "Оппс… Заблудился?")
        self.assertEqual(row["comments_loaded_count"], 0)
        self.assertFalse(row["comments_complete"])
        self.assertEqual(row["nuxt_errors"]["error"]["statusCode"], 404)

    def test_devalue_constants_and_null_prototype_object(self):
        decoder = extractor.DevalueDecoder("[null]")
        self.assertIsNone(decoder.hydrate(-1))
        self.assertIsNone(decoder.hydrate(-2))
        self.assertEqual(decoder.hydrate(-3), "NaN")
        self.assertEqual(decoder.hydrate(-4), "Infinity")
        self.assertEqual(decoder.hydrate(-5), "-Infinity")
        self.assertEqual(math.copysign(1, decoder.hydrate(-6)), -1)

        raw = json.dumps(
            [
                ["ShallowReactive", 1],
                {"error": 2},
                ["null", "statusCode", 3, "message", 4],
                404,
                "Page not found",
            ]
        )
        error = extractor.DevalueDecoder(raw).root_value("error")
        self.assertEqual(error, {"statusCode": 404, "message": "Page not found"})

    def test_empty_article_content_is_ok_with_warning(self):
        self.write(10, article_page(content="", comments=[], comments_count=0))

        row = self.parse_one(10)

        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["content_html"], "")
        self.assertTrue(row["comments_complete"])
        self.assertIn("article has empty content", row["parse_warnings"])

    def test_malformed_nuxt_becomes_parse_error_row(self):
        self.write(
            30,
            '<html><title>Broken</title><script id="__NUXT_DATA__">[bad</script></html>',
        )

        row = self.parse_one(30)

        self.assertEqual(row["status"], "parse_error")
        self.assertIn("NuxtDecodeError", row["error_message"])
        self.assertEqual(row["source_id"], 30)
        self.assertEqual(len(row["source_sha256"]), 64)

    def test_output_is_one_to_one_sorted_deterministic_and_cached(self):
        self.write(20, not_found_page())
        self.write(10, article_page())

        first = extractor.extract_news(
            self.input, self.output, workers=1, progress_every=0
        )
        first_bytes = (self.output / "news.jsonl.gz").read_bytes()
        rows = self.read_rows()

        self.assertEqual([row["source_id"] for row in rows], [10, 20])
        self.assertEqual(first["record_count"], 2)
        self.assertEqual(first["source_file_count"], 2)
        self.assertEqual(first["source_min_id"], 10)
        self.assertEqual(first["source_max_id"], 20)
        self.assertFalse(first["source_ids_contiguous"])
        self.assertEqual(first["source_status_counts"], {"not_found": 1, "ok": 1})
        self.assertEqual(first["comments_loaded_count"], 3)
        self.assertEqual(first["comments_meta_count"], 3)
        self.assertEqual(first["comments_incomplete_page_count"], 0)
        self.assertEqual(first["comments_missing_count"], 0)
        self.assertEqual(first["jsonl"], "news.jsonl.gz")
        self.assertEqual(
            first["jsonl_sha256"], hashlib.sha256(first_bytes).hexdigest()
        )

        cached = extractor.extract_news(
            self.input, self.output, workers=1, progress_every=0
        )
        self.assertEqual(cached["status"], "cached")
        forced = extractor.extract_news(
            self.input, self.output, workers=2, force=True, progress_every=0
        )
        self.assertEqual(forced["status"], "extracted")
        self.assertEqual(first_bytes, (self.output / "news.jsonl.gz").read_bytes())

    def test_manifest_aggregates_missing_nested_comments(self):
        comments = [{"comment": {"id": 1}, "comments": []}]
        self.write(10, article_page(comments=comments, comments_count=2))

        manifest = extractor.extract_news(
            self.input, self.output, workers=1, progress_every=0
        )

        self.assertEqual(manifest["comments_loaded_count"], 1)
        self.assertEqual(manifest["comments_meta_count"], 2)
        self.assertEqual(manifest["comments_incomplete_page_count"], 1)
        self.assertEqual(manifest["comments_missing_count"], 1)
        self.assertTrue(manifest["source_parse_complete"])

    def test_rejects_unexpected_source_files(self):
        (self.input / "README.txt").write_text("unexpected", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "Unexpected news source file"):
            extractor.discover_inputs(self.input)

    def test_rejects_missing_id_in_expected_source_range(self):
        self.write(10, article_page(10))
        self.write(12, article_page(12))

        with self.assertRaisesRegex(ValueError, r"coverage mismatch.*missing=\[11\]"):
            extractor.extract_news(
                self.input,
                self.output,
                workers=1,
                progress_every=0,
                expected_min_id=10,
                expected_max_id=12,
            )


if __name__ == "__main__":
    unittest.main()
