import sys
import types
import unittest


try:
    import yt.wrapper  # noqa: F401
except ModuleNotFoundError:
    yt_package = types.ModuleType("yt")
    yt_wrapper = types.ModuleType("yt.wrapper")
    yt_wrapper.YtClient = object
    yt_package.wrapper = yt_wrapper
    sys.modules["yt"] = yt_package
    sys.modules["yt.wrapper"] = yt_wrapper

import move_shikimori_yt_to_film_data as migration


class FakeClient:
    def __init__(self):
        self.nodes = {
            "//old": {"id": "root-id", "type": "map_node"},
            "//old/animes": {"id": "animes-id", "type": "table"},
            "//old/reviews": {"id": "reviews-id", "type": "table"},
        }
        self.moves = []
        self.creates = []

    def exists(self, path):
        return path in self.nodes

    def get(self, path):
        node_path, attribute = path.rsplit("/@", 1)
        return self.nodes[node_path][attribute]

    def list(self, path):
        prefix = path.rstrip("/") + "/"
        return sorted(
            candidate[len(prefix) :]
            for candidate in self.nodes
            if candidate.startswith(prefix)
            and "/" not in candidate[len(prefix) :]
        )

    def create(self, node_type, path, recursive=False):
        self.creates.append((node_type, path, recursive))
        if recursive:
            parts = path[2:].split("/")
            for index in range(1, len(parts) + 1):
                candidate = "//" + "/".join(parts[:index])
                self.nodes.setdefault(
                    candidate,
                    {"id": "created-" + candidate, "type": "map_node"},
                )
        else:
            self.nodes[path] = {"id": "created-" + path, "type": node_type}

    def move(self, source, target):
        if target in self.nodes:
            raise RuntimeError("target exists")
        self.moves.append((source, target))
        moved = {
            target + path[len(source) :]: attributes
            for path, attributes in self.nodes.items()
            if path == source or path.startswith(source + "/")
        }
        self.nodes = {
            path: attributes
            for path, attributes in self.nodes.items()
            if path != source and not path.startswith(source + "/")
        }
        self.nodes.update(moved)


class MoveShikimoriYtToFilmDataTest(unittest.TestCase):
    def test_moves_whole_subtree_and_preserves_ids(self):
        client = FakeClient()

        result = migration.move_subtree(client, "//old/", "//film_data/shikimori")

        self.assertEqual(result["status"], "moved")
        self.assertEqual(result["children"], ["animes", "reviews"])
        self.assertFalse(client.exists("//old"))
        self.assertEqual(client.get("//film_data/shikimori/@id"), "root-id")
        self.assertEqual(
            client.get("//film_data/shikimori/reviews/@id"), "reviews-id"
        )
        self.assertEqual(client.moves, [("//old", "//film_data/shikimori")])
        self.assertEqual(
            client.creates, [("map_node", "//film_data", True)]
        )

    def test_target_conflict_fails_before_move(self):
        client = FakeClient()
        client.nodes["//target"] = {"id": "existing", "type": "map_node"}

        with self.assertRaisesRegex(ValueError, "already exists"):
            migration.move_subtree(client, "//old", "//target")

        self.assertEqual(client.moves, [])
        self.assertTrue(client.exists("//old"))

    def test_missing_source_and_valid_target_is_idempotent(self):
        client = FakeClient()
        client.move("//old", "//target")

        result = migration.move_subtree(
            client,
            "//old",
            "//target",
            expected_children={"animes", "reviews"},
        )

        self.assertEqual(result["status"], "already_moved")
        self.assertEqual(result["children"], ["animes", "reviews"])
        self.assertEqual(len(client.moves), 1)

    def test_idempotent_target_verifies_expected_children(self):
        client = FakeClient()
        client.move("//old", "//target")

        with self.assertRaisesRegex(RuntimeError, "children differ"):
            migration.move_subtree(
                client,
                "//old",
                "//target",
                expected_children={"animes", "people", "reviews"},
            )

    def test_dry_run_does_not_create_parent_or_move(self):
        client = FakeClient()

        result = migration.move_subtree(
            client, "//old", "//missing/target", dry_run=True
        )

        self.assertEqual(result["status"], "planned")
        self.assertEqual(client.creates, [])
        self.assertEqual(client.moves, [])

    def test_rejects_non_map_source_and_nested_paths(self):
        client = FakeClient()
        with self.assertRaisesRegex(ValueError, "contain one another"):
            migration.move_subtree(client, "//old", "//old/new")
        with self.assertRaisesRegex(ValueError, "map_node"):
            migration.move_subtree(client, "//old/animes", "//target")


if __name__ == "__main__":
    unittest.main()
