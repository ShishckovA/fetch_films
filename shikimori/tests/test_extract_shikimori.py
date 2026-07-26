import csv
import json
import tempfile
import unittest
from pathlib import Path

from extract_shikimori import COLUMNS, export_tsv, parse_file


FULL_HTML = """<!doctype html>
<html>
  <head>
    <title>Example Anime / Аниме</title>
    <link rel="canonical" href="https://shikimori.io/animes/z404-example?locale=ru">
    <meta property="og:type" content="video.tv_show">
    <meta property="og:image" content="https://cdn.example/previews/404.jpg">
    <meta property="video:duration" content="1440">
    <meta property="video:release_date" content="2024-06-30">
    <meta property="video:tag" content="Драма">
  </head>
  <body class="p-animes p-animes-show" data-locale="ru"
        data-server_time="2026-07-11T12:00:00+03:00">
    <h1>Пример / Example Anime</h1>
    <meta itemprop="url" content="https://shikimori.io/animes/z404-example">
    <meta itemprop="headline" content="Example Anime">
    <meta itemprop="alternativeHeadline" content="Пример">
    <meta itemprop="dateCreated" content="2024-01-01">
    <div class="b-breadcrumbs"><a href="/animes">Аниме</a></div>
    <div class="b-db_entry-poster" data-href="https://img.example/original.jpg">
      <meta itemprop="image" content="https://img.example/main.webp">
      <img src="https://img.example/preview.jpg" width="225" height="318">
    </div>
    <div class="b-user_rate" data-entry='{"id":404,"episodes":12,"chapters":null,"volumes":null}'></div>
    <div class="b-entry-info">
      <div class="line"><div class="key">Тип:</div><div class="value">TV Сериал</div></div>
      <div class="line"><div class="key">Эпизоды:</div><div class="value">12</div></div>
      <div class="line"><div class="key">Длительность эпизода:</div><div class="value">24 мин.</div></div>
      <div class="line"><div class="key">Статус:</div><div class="value"><span class="b-anime_status_tag released" data-text="вышло"></span> в 2024 г.</div></div>
      <div class="line"><div class="key">Жанры:</div><div class="value">
        <a href="https://shikimori.io/animes/genre/8-Drama"><span class="genre-en">Drama</span><span class="genre-ru">Драма</span></a>
      </div></div>
      <div class="line"><div class="key">Тема:</div><div class="value">
        <a href="https://shikimori.io/animes/genre/23-School"><span class="genre-en">School</span><span class="genre-ru">Школа</span></a>
      </div></div>
      <div class="line"><div class="key">Рейтинг:</div><div class="value">PG-13</div></div>
      <div class="line"><div class="key">Первоисточник:</div><div class="value">Манга</div></div>
      <div class="line"><div class="key">Альтернативные названия:</div><div class="value"><span class="other-names" data-clickloaded-url="/animes/z404-example/other_names">···</span></div></div>
    </div>
    <div class="c-info-right">
      <div itemprop="aggregateRating">
        <meta itemprop="bestRating" content="10">
        <meta itemprop="ratingValue" content="8.5">
        <meta itemprop="ratingCount" content="1234">
        <div class="b-rate"><div class="score-notice">Отлично</div></div>
      </div>
      <a href="https://shikimori.io/animes/studio/11-Example" title="Аниме студии Example Studio"><img class="studio-logo" src="studio.png"></a>
    </div>
    <div class="description-current">
      <div itemprop="description">Строка с\nпереносом и\tтабуляцией.</div>
      <div class="b-source"><div class="b-user16"><a href="/author">Автор</a></div></div>
    </div>
    <div id="rates_scores_stats" data-stats='[["10",5],["9",3],["1",1]]'></div>
    <div id="rates_statuses_stats" data-stats='[["planned",10],["completed",20]]'></div>
    <div class="total-rates">В списках у 30 человек</div>
    <div class="subheadline"><a title="Все комментарии" href="https://shikimori.io/forum/animanga/anime-z404-example/999-discussion">Комментарии<div class="count">7</div></a></div>
    <div class="b-comments"><div class="b-comment" id="77" data-user_id="5" data-user_nickname="User" itemprop="comment">
      <div class="inner"><header><a class="name" href="/User">User</a><time datetime="2024-02-03T10:00:00+03:00"></time></header>
      <div class="body" itemprop="text">Полезный <a href="https://example.test/comment">комментарий</a>.</div></div>
    </div></div>
    <div class="subheadline">В избранном <span class="count">4</span></div>
    <div class="b-menu-links menu-topics-block history"><div class="subheadline">Новости</div><div class="block">
      <a class="b-menu-line entry" itemprop="discussionUrl" href="https://shikimori.io/forum/news/123-news"><time datetime="2024-03-01T10:00:00+03:00">1 марта</time><span class="name">Новость</span></a>
    </div></div>
    <div class="b-reviews_navigation">
      <div class="navigation-node" data-opinion=""><div class="count">6</div></div>
      <div class="navigation-node" data-opinion="positive"><div class="count">5</div></div>
      <div class="navigation-node" data-opinion="negative"><div class="count">1</div></div>
    </div>
    <div class="b-external_link myanimelist"><span data-href="https://myanimelist.net/anime/404">MyAnimeList</span></div>
    <div class="b-external_link official_site"><span data-href="https://example.test/anime">Официальный сайт</span></div>
  </body>
</html>"""

REDIRECT_HTML = """<!doctype html><html><head><title>404</title></head><body>
<p class="error-404">302</p><h1>Страница переехала</h1>
<a href="https://shikimori.io/animes/z20-naruto">новая ссылка</a>
</body></html>"""

NOT_FOUND_HTML = """<!doctype html><html><head><title>404</title></head><body>
<p class="error-404">404</p><h1>Страница не найдена</h1>
</body></html>"""

RESTRICTED_HTML = """<!doctype html><html><body class="p-age_restricted p-animes p-animes-show">
<h1>Контент ограничен 18+</h1></body></html>"""


class ExtractShikimoriTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def parse(self, name, html):
        path = self.root / name
        path.write_text(html, encoding="utf-8")
        return parse_file(str(path), str(self.root))

    def test_extracts_full_page_metadata_and_statistics(self):
        row = self.parse("z404.html", FULL_HTML)

        self.assertEqual(row["page_status"], "ok")
        self.assertEqual(row["canonical_id"], "404")
        self.assertEqual(row["canonical_url"], "https://shikimori.io/animes/z404-example")
        self.assertEqual(row["title"], "Example Anime")
        self.assertEqual(row["title_russian"], "Пример")
        self.assertEqual(row["episodes"], 12)
        self.assertEqual(row["duration_seconds"], 1440)
        self.assertEqual(row["status_code"], "released")
        self.assertEqual(row["genres_en"], "Drama")
        self.assertEqual(row["genres_ru"], "Драма")
        self.assertEqual(row["themes_en"], "School")
        self.assertEqual(row["studio_ids"], "11")
        self.assertEqual(row["score"], "8.5")
        self.assertEqual(row["rating_count"], 1234)
        self.assertEqual(row["score_10_count"], 5)
        self.assertEqual(row["score_8_count"], 0)
        self.assertEqual(row["score_votes_total"], 9)
        self.assertEqual(row["list_completed_count"], 20)
        self.assertEqual(row["user_list_total"], 30)
        self.assertEqual(row["comments_count"], 7)
        self.assertEqual(row["comments_preview_count"], 1)
        self.assertEqual(json.loads(row["comments_json"])[0]["id"], "77")
        self.assertEqual(row["news_count"], 1)
        self.assertEqual(row["discussion_topic_id"], "999")
        self.assertEqual(row["reviews_positive_count"], 5)
        self.assertEqual(row["myanimelist_id"], "404")
        self.assertEqual(row["official_site_url"], "https://example.test/anime")
        self.assertEqual(row["alternative_titles"], "")
        self.assertEqual(row["description"], "Строка с переносом и табуляцией.")
        self.assertEqual(json.loads(row["genres_json"])[0]["id"], "8")

    def test_extracts_episode_progress_and_scheduled_dates(self):
        html = FULL_HTML.replace(
            '<div class="value">12</div>', '<div class="value">1 / 12</div>', 1
        ).replace(
            '<div class="line"><div class="key">Альтернативные названия:</div>',
            '<div class="line"><div class="key">Первый эпизод:</div><div class="value"><span class="local-time" data-datetime="2024-01-01T10:00:00+03:00"></span></div></div>'
            '<div class="line"><div class="key">Следующий эпизод:</div><div class="value"><span class="local-time" data-datetime="2024-01-08T10:00:00+03:00"></span></div></div>'
            '<div class="line"><div class="key">Альтернативные названия:</div>',
        )
        row = self.parse("progress.html", html)

        self.assertEqual(row["episodes_aired"], 1)
        self.assertEqual(row["episodes_total"], 12)
        self.assertEqual(row["first_episode_at"], "2024-01-01T10:00:00+03:00")
        self.assertEqual(row["next_episode_at"], "2024-01-08T10:00:00+03:00")

    def test_scopes_release_date_and_keeps_other_description(self):
        html = FULL_HTML.replace(
            '<meta itemprop="dateCreated" content="2024-01-01">',
            '<meta itemprop="datePublished" content="2024-01-02">',
        ).replace(
            '<div itemprop="description">Строка с\nпереносом и\tтабуляцией.</div>',
            '<div itemprop="description"><div class="b-nothing_here">Нет описания</div></div>',
        ).replace(
            '<div id="rates_scores_stats"',
            '<div class="description-other"><div class="text">English description.</div><div class="b-source"><div class="source"><div class="val"><a href="https://source.test">Source</a></div></div></div></div>'
            '<div class="cc-similar"><article class="b-catalog_entry" id="9"><meta itemprop="dateCreated" content="1999-01-01"></article></div>'
            '<div id="rates_scores_stats"',
        )
        row = self.parse("descriptions.html", html)

        self.assertEqual(row["aired_on"], "2024-01-02")
        self.assertEqual(row["aired_on_kind"], "datePublished")
        self.assertEqual(row["description"], "")
        self.assertEqual(row["has_description"], 0)
        self.assertEqual(row["description_other"], "English description.")
        self.assertEqual(
            json.loads(row["description_other_source_json"])["source_url"],
            "https://source.test",
        )

    def test_og_404_image_does_not_make_valid_anime_a_404(self):
        row = self.parse("404.html", FULL_HTML)
        self.assertEqual(row["page_status"], "ok")
        self.assertEqual(row["og_image_url"], "https://cdn.example/previews/404.jpg")

    def test_classifies_redirect_not_found_and_restricted_pages(self):
        redirect = self.parse("20.html", REDIRECT_HTML)
        missing = self.parse("21.html", NOT_FOUND_HTML)
        restricted = self.parse("22.html", RESTRICTED_HTML)

        self.assertEqual(redirect["page_status"], "redirect")
        self.assertEqual(redirect["redirect_id"], "20")
        self.assertEqual(missing["page_status"], "not_found")
        self.assertEqual(restricted["page_status"], "age_restricted")
        self.assertEqual(restricted["is_adult"], 1)

    def test_named_adult_page_gets_id_from_canonical_url(self):
        html = FULL_HTML.replace(
            "<h1>Пример / Example Anime</h1>", "<h1>Пример / Example Anime 18+</h1>"
        )
        row = self.parse("Example Anime _ Аниме.html", html)

        self.assertEqual(row["source_variant"], "named")
        self.assertEqual(row["source_requested_id"], "")
        self.assertEqual(row["canonical_id"], "404")
        self.assertEqual(row["is_adult"], 1)

    def test_empty_and_missing_inputs_become_diagnostic_rows(self):
        empty = self.parse("empty.html", "")
        missing_path = self.root / "missing.html"
        missing = parse_file(str(missing_path), str(self.root))

        self.assertEqual(empty["page_status"], "unknown")
        self.assertEqual(missing["page_status"], "parse_error")
        self.assertIn("FileNotFoundError", missing["parse_error"])

    def test_export_is_deterministic_and_writes_valid_tsv(self):
        (self.root / "2.html").write_text(REDIRECT_HTML, encoding="utf-8")
        (self.root / "1.html").write_text(FULL_HTML, encoding="utf-8")
        output = self.root / "result.tsv"

        counts = export_tsv(
            self.root, output, workers=1, progress_every=0
        )
        with output.open(encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file, delimiter="\t")
            rows = list(reader)

        self.assertEqual(reader.fieldnames, COLUMNS)
        self.assertEqual([row["source_file"] for row in rows], ["1.html", "2.html"])
        self.assertEqual(rows[0]["description"], "Строка с переносом и табуляцией.")
        self.assertEqual(counts, {"ok": 1, "redirect": 1})

    def test_resume_keeps_prior_rows_and_complete_summary(self):
        (self.root / "2.html").write_text(REDIRECT_HTML, encoding="utf-8")
        (self.root / "1.html").write_text(FULL_HTML, encoding="utf-8")
        first_output = self.root / "first.tsv"
        output = self.root / "resumed.tsv"
        export_tsv(self.root, first_output, workers=1, limit=1, progress_every=0)
        first_output.replace(Path(str(output) + ".part"))

        counts = export_tsv(
            self.root, output, workers=1, resume=True, progress_every=0
        )
        with output.open(encoding="utf-8", newline="") as file:
            rows = list(csv.DictReader(file, delimiter="\t"))

        self.assertEqual([row["source_file"] for row in rows], ["1.html", "2.html"])
        self.assertEqual(counts, {"ok": 1, "redirect": 1})


if __name__ == "__main__":
    unittest.main()
