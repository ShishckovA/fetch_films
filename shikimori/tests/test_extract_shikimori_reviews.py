import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import extract_shikimori_reviews as extractor


def review_html(
    *,
    anime_slug="z404-example",
    review_id=100,
    topic_id=500,
    author_id=42,
    snapshot="Old Name",
    visible="Current Name",
    votes_for=10,
    votes_against=2,
    opinion="positive",
    score=0,
    status="completed",
    body="Полный текст",
    ongoing=False,
):
    score_html = (
        f'<div class="stars score score-{score}"></div>' if score is not None else ""
    )
    status_html = (
        f'<div class="b-add_to_list {status}"></div>' if status is not None else ""
    )
    ongoing_html = (
        '<div class="is_written_before_release-container">'
        '<div class="is_written_before_release"></div></div>'
        if ongoing
        else ""
    )
    return f"""
<article class="b-topic b-review-topic" id="{topic_id}"
 data-track_topic="{topic_id}"
 data-url="https://shikimori.io/animes/{anime_slug}/reviews/{review_id}"
 data-user_id="{author_id}" data-user_nickname="{snapshot}">
 <meta content="{snapshot}" itemprop="author">
 <meta content="Example Anime" itemprop="name">
 <meta content="Example Anime" itemprop="headline">
 <meta content="https://shikimori.io/animes/{anime_slug}/reviews/{review_id}" itemprop="url">
 <meta content="2024-01-01T10:00:00+03:00" itemprop="dateCreated">
 <meta content="2024-01-01T10:00:01+03:00" itemprop="datePublished">
 <meta content="2024-01-02T10:00:00+03:00" itemprop="dateModified">
 <header>
  <a class="author-link" href="/users/{author_id}"><img src="/avatar.png" srcset="/avatar2.png 2x"></a>
  <div class="review-details"><a class="name" href="/users/{author_id}">{visible}</a></div>
  <span class="votes-for"> {votes_for} </span>
  <span class="votes-against"> {votes_against} </span>
  <span class="comments"> 1 234 </span>
  <div class="review-info"><div class="opinion {opinion}"></div>{score_html}{status_html}</div>
 </header>
 {ongoing_html}
 <div><div class="body" itemprop="text">{body}</div><footer>vote</footer></div>
</article>
"""


def write_json_page(path: Path, content: str, *, postloader=False):
    payload = {"content": content, "JS_EXPORTS": {"polls": []}}
    if postloader:
        payload["postloader"] = "<div>next</div>"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


class ExtractShikimoriReviewsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.input = self.root / "reviews"
        self.output = self.root / "parsed"
        self.input.mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    def read_rows(self):
        with gzip.open(self.output / "reviews.jsonl.gz", "rt", encoding="utf-8") as file:
            return [json.loads(line) for line in file]

    def test_extracts_full_body_nested_content_and_all_fields(self):
        directory = self.input / "z404-example"
        directory.mkdir()
        nested_body = """
Начало<br><div class="b-spoiler_block"><span>Заголовок</span><div>Скрытый текст</div></div>
<span class="b-spoiler_inline"><span>inline secret</span></span>
<a href="/animes/1-test" title="Title">ссылка</a>
<a class="b-image" href="/image-full.jpg"><img src="/image.jpg" data-width="1280" data-height="720"></a>
<article class="b-catalog_entry"><div class="body">nested non-review article</div></article>
"""
        content = review_html(
            review_id=100,
            votes_for=7,
            votes_against=2,
            body=nested_body,
            ongoing=True,
        ) + review_html(
            review_id=101,
            topic_id=501,
            votes_for=0,
            votes_against=0,
            score=None,
            status=None,
            body=(
                'Второй<div class="stars score score-10"></div>'
                '<div class="b-add_to_list dropped"></div>'
                '<div class="is_written_before_release"></div>'
            ),
        )
        write_json_page(directory / "1.json", content)

        manifest = extractor.extract_reviews(
            self.input, self.output, workers=1, progress_every=0
        )
        rows = self.read_rows()

        self.assertEqual(len(rows), 2)
        self.assertEqual(manifest["record_count"], 2)
        self.assertEqual(manifest["top_50_record_count"], 2)
        first = rows[0]
        self.assertEqual(set(first), set(extractor.FIELDS))
        self.assertEqual(first["review_id"], 100)
        self.assertEqual(first["usefulness_rank"], 1)
        self.assertEqual(first["anime_id"], 404)
        self.assertEqual(first["anime_slug"], "z404-example")
        self.assertEqual(first["anime_url"], "https://shikimori.io/animes/z404-example")
        self.assertEqual(first["author_nickname"], "Current Name")
        self.assertEqual(first["author_nickname_snapshot"], "Old Name")
        self.assertEqual(first["author_avatar_srcset"], "/avatar2.png 2x")
        self.assertEqual(first["user_score"], 0)
        self.assertEqual(first["user_list_status"], "completed")
        self.assertEqual(first["comments_count"], 1234)
        self.assertTrue(first["is_written_before_release"])
        self.assertIn("Скрытый текст", first["body_text"])
        self.assertIn("nested non-review article", first["body_text"])
        self.assertIn("b-spoiler_block", first["body_html"])
        self.assertEqual(first["inline_spoilers_count"], 1)
        self.assertEqual(first["block_spoilers_count"], 1)
        self.assertEqual(first["body_links"][0]["url"], "https://shikimori.io/animes/1-test")
        self.assertEqual(first["body_images"][0]["url"], "https://shikimori.io/image.jpg")
        self.assertNotIn("user_score", rows[1])
        self.assertNotIn("user_list_status", rows[1])
        self.assertNotIn("usefulness_ratio", rows[1])
        self.assertFalse(rows[1]["is_written_before_release"])

    def test_ranking_is_deterministic_and_keeps_all_rows(self):
        directory = self.input / "20-ranking"
        directory.mkdir()
        content = "".join(
            [
                review_html(anime_slug="20-ranking", review_id=1, topic_id=11, votes_for=5, votes_against=3),
                review_html(anime_slug="20-ranking", review_id=2, topic_id=12, votes_for=6, votes_against=4),
                review_html(anime_slug="20-ranking", review_id=3, topic_id=13, votes_for=6, votes_against=4),
                review_html(anime_slug="20-ranking", review_id=4, topic_id=14, votes_for=100, votes_against=99),
            ]
        )
        write_json_page(directory / "1.json", content, postloader=True)

        manifest = extractor.extract_reviews(self.input, self.output, progress_every=0)
        rows = self.read_rows()

        self.assertEqual([row["review_id"] for row in rows], [3, 2, 1, 4])
        self.assertEqual([row["usefulness_rank"] for row in rows], [1, 2, 3, 4])
        self.assertEqual(extractor.RANKING_METHOD, "usefulness_score DESC, votes_for DESC, review_id DESC")
        self.assertEqual(manifest["terminal_postloader_pages"], 1)
        self.assertTrue(manifest["source_pagination_complete"])
        self.assertIn("last_page_has_postloader", manifest["source_issue_details"][0])

    def test_classifies_terminal_error_transient_and_empty_sources(self):
        empty = self.input / "31687-"
        empty.mkdir()
        directory = self.input / "2-errors"
        directory.mkdir()
        (directory / "1.json").write_text(
            '<html><body class="p-animes p-animes-show">'
            '<meta itemprop="url" content="https://shikimori.io/animes/2-errors">'
            '<div class="navigation-node-all"><div class="count">0</div></div>'
            '</body></html>',
            encoding="utf-8",
        )
        (directory / "2.json").write_text(
            json.dumps({"status": 404, "error": "Not Found"}), encoding="utf-8"
        )
        (directory / "3.json").write_text("age_restricted", encoding="utf-8")
        write_json_page(
            directory / "4.json",
            "<p>Отзывы временно недоступны, на сайте проводятся технические работы.</p>",
            postloader=True,
        )
        (directory / "5.json").write_text("not json", encoding="utf-8")
        (directory / "6.json").write_text(
            json.dumps({"status": 500, "error": "Error"}), encoding="utf-8"
        )
        (directory / "7.json").write_text(
            "<html><body><h1>502 Bad Gateway</h1></body></html>", encoding="utf-8"
        )

        manifest = extractor.extract_reviews(
            self.input, self.output, progress_every=0
        )

        self.assertEqual(self.read_rows(), [])
        self.assertTrue(manifest["extraction_complete"])
        self.assertFalse(manifest["source_pagination_complete"])
        self.assertEqual(manifest["source_status_counts"]["empty_directory"], 1)
        self.assertEqual(manifest["source_status_counts"]["html_terminal"], 1)
        self.assertEqual(manifest["source_status_counts"]["api_error_404"], 1)
        self.assertEqual(manifest["source_status_counts"]["age_restricted"], 1)
        self.assertEqual(manifest["source_status_counts"]["transient_unavailable"], 1)
        self.assertEqual(manifest["source_status_counts"]["malformed_json"], 1)
        self.assertEqual(manifest["source_status_counts"]["api_error_500"], 1)
        self.assertEqual(manifest["source_status_counts"]["html_unknown"], 1)
        self.assertTrue(
            any("2-errors/6.json: api_error_500" in issue for issue in manifest["source_issue_details"])
        )

    def test_deterministic_gzip_and_inventory_cache(self):
        directory = self.input / "3-cache"
        directory.mkdir()
        write_json_page(
            directory / "1.json",
            review_html(anime_slug="3-cache", review_id=30, topic_id=31),
        )
        first = extractor.extract_reviews(self.input, self.output, progress_every=0)
        first_bytes = (self.output / "reviews.jsonl.gz").read_bytes()
        first_hash = hashlib.sha256(first_bytes).hexdigest()
        self.assertEqual(first["jsonl_sha256"], first_hash)

        cached = extractor.extract_reviews(self.input, self.output, progress_every=0)
        self.assertEqual(cached["status"], "cached")
        manifest_path = self.output / "manifest.json"
        stale_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        stale_manifest["ranking_method"] = "stale"
        manifest_path.write_text(json.dumps(stale_manifest), encoding="utf-8")
        invalidated = extractor.extract_reviews(
            self.input, self.output, progress_every=0
        )
        self.assertEqual(invalidated["status"], "extracted")
        forced = extractor.extract_reviews(
            self.input, self.output, force=True, progress_every=0
        )
        self.assertEqual(first_bytes, (self.output / "reviews.jsonl.gz").read_bytes())
        self.assertEqual(forced["jsonl_sha256"], first_hash)

    def test_malformed_review_is_fatal_and_part_is_removed(self):
        directory = self.input / "4-bad"
        directory.mkdir()
        malformed = review_html(anime_slug="4-bad", review_id=40, topic_id=41).replace(
            '<div class="body" itemprop="text">Полный текст</div>', ""
        )
        write_json_page(directory / "1.json", malformed)

        with self.assertRaisesRegex(extractor.ReviewParseError, "missing review body"):
            extractor.extract_reviews(self.input, self.output, progress_every=0)
        self.assertFalse((self.output / "reviews.jsonl.gz.part").exists())
        self.assertFalse((self.output / "reviews.jsonl.gz").exists())


if __name__ == "__main__":
    unittest.main()
