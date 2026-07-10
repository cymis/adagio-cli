import tempfile
import unittest
from pathlib import Path

from adagio.executors.signature import (
    compute_input_signature,
    environment_reference,
    hash_file_bytes,
)


def _write(path: Path, text: str) -> str:
    path.write_text(text, encoding="utf-8")
    return str(path)


class InputSignatureTests(unittest.TestCase):
    def test_signature_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a = _write(root / "a.qza", "alpha")
            first = compute_input_signature(
                params={"metric": "jaccard", "threads": 4},
                inputs={"table": a},
                env="docker:img@sha256:deadbeef",
            )
            second = compute_input_signature(
                params={"threads": 4, "metric": "jaccard"},  # key order differs
                inputs={"table": a},
                env="docker:img@sha256:deadbeef",
            )
        self.assertEqual(first, second)
        self.assertTrue(first.startswith("sha256:"))

    def test_param_change_changes_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            a = _write(Path(tmp) / "a.qza", "alpha")
            base = compute_input_signature(
                params={"metric": "jaccard"}, inputs={"t": a}, env="e"
            )
            changed = compute_input_signature(
                params={"metric": "braycurtis"}, inputs={"t": a}, env="e"
            )
        self.assertNotEqual(base, changed)

    def test_env_change_changes_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            a = _write(Path(tmp) / "a.qza", "alpha")
            base = compute_input_signature(
                params={"metric": "jaccard"}, inputs={"t": a}, env="docker:img@d1"
            )
            changed = compute_input_signature(
                params={"metric": "jaccard"}, inputs={"t": a}, env="docker:img@d2"
            )
        self.assertNotEqual(base, changed)

    def test_input_content_change_changes_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "a.qza"
            first = compute_input_signature(
                params={}, inputs={"t": _write(path, "one")}, env="e"
            )
            second = compute_input_signature(
                params={}, inputs={"t": _write(path, "two")}, env="e"
            )
        self.assertNotEqual(first, second)

    def test_uri_input_is_identity_not_hashed(self) -> None:
        sig = compute_input_signature(
            params={}, inputs={"t": "s3://bucket/obj.qza"}, env="e"
        )
        # A URI is used verbatim (never fetched); a different URI => different hash.
        other = compute_input_signature(
            params={}, inputs={"t": "s3://bucket/other.qza"}, env="e"
        )
        self.assertNotEqual(sig, other)

    def test_missing_file_does_not_raise(self) -> None:
        sig = compute_input_signature(
            params={}, inputs={"t": "/no/such/file.qza"}, env="e"
        )
        self.assertTrue(sig.startswith("sha256:"))

    def test_hash_file_bytes_matches_known_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x"
            path.write_text("abc", encoding="utf-8")
            # sha256("abc")
            self.assertEqual(
                hash_file_bytes(path),
                "sha256:ba7816bf8f01cfea414140de5dae2223"
                "b00361a396177a9cb410ff61f20015ad",
            )

    def test_collection_input_order_matters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a = _write(root / "a.qza", "aaa")
            b = _write(root / "b.qza", "bbb")
            forward = compute_input_signature(
                params={}, inputs={"tables": [a, b]}, env="e"
            )
            reverse = compute_input_signature(
                params={}, inputs={"tables": [b, a]}, env="e"
            )
        self.assertNotEqual(forward, reverse)

    def test_environment_reference_docker_with_digest(self) -> None:
        self.assertEqual(
            environment_reference(
                kind="docker", reference="img:tag", digest="sha256:abc"
            ),
            "docker:img:tag@sha256:abc",
        )
        self.assertEqual(
            environment_reference(kind="docker", reference="img:tag"),
            "docker:img:tag",
        )
        self.assertEqual(
            environment_reference(kind="conda", reference="q2-2026"),
            "conda:q2-2026",
        )


if __name__ == "__main__":
    unittest.main()
