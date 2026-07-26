import gzip
import json
import tempfile
import unittest
from pathlib import Path

import upload_shikimori_graphql_to_yt as uploader


class UploadShikimoriGraphqlToYtTest(unittest.TestCase):
    def test_schemas_preserve_all_columns_and_types(self):
        self.assertEqual(
            {entity: len(uploader.table_schema(entity)) for entity in uploader.ENTITY_FIELDS},
            {"animes": 45, "characters": 17, "people": 17},
        )
        anime_types = {
            column["name"]: column["type"]
            for column in uploader.table_schema("animes")
        }
        self.assertEqual(anime_types["id"], "int64")
        self.assertEqual(anime_types["score"], "double")
        self.assertEqual(anime_types["isCensored"], "boolean")
        self.assertEqual(anime_types["related"], "any")
        self.assertEqual(anime_types["descriptionHtml"], "string")
        self.assertEqual(
            uploader.table_schema("animes")[0]["sort_order"], "ascending"
        )

    def test_convert_row_keeps_nested_yson_and_omits_nulls(self):
        source = {field: None for field in uploader.ENTITY_FIELDS["people"]}
        source.update(
            {
                "id": "100",
                "malId": "42",
                "name": "Name",
                "synonyms": ["Alias"],
                "isMangaka": True,
                "isProducer": False,
                "isSeyu": False,
                "birthOn": {"year": 1980, "month": None},
                "deceasedOn": {"year": None},
                "website": "",
                "url": "https://example.test",
            }
        )

        row = uploader.convert_row("people", source, 1)

        self.assertEqual(row["id"], 100)
        self.assertEqual(row["malId"], 42)
        self.assertEqual(row["synonyms"], ["Alias"])
        self.assertEqual(row["birthOn"], {"year": 1980, "month": None})
        self.assertNotIn("createdAt", row)

    def test_iter_rows_rejects_non_increasing_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "characters.jsonl.gz"
            base = {field: None for field in uploader.ENTITY_FIELDS["characters"]}
            rows = []
            for record_id in ("2", "1"):
                row = dict(base)
                row.update(
                    {
                        "id": record_id,
                        "name": "name",
                        "synonyms": [],
                        "descriptionHtml": "",
                        "isAnime": True,
                        "isManga": False,
                        "isRanobe": False,
                        "createdAt": "date",
                        "updatedAt": "date",
                        "url": "url",
                    }
                )
                rows.append(row)
            with gzip.open(path, "wt", encoding="utf-8") as output:
                for row in rows:
                    output.write(json.dumps(row) + "\n")

            with self.assertRaisesRegex(ValueError, "strictly increasing"):
                list(
                    uploader.iter_rows(
                        path,
                        "characters",
                        uploader.RowStats(),
                        progress_every=0,
                    )
                )


if __name__ == "__main__":
    unittest.main()
