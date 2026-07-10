import tempfile
import unittest
from pathlib import Path

from adagio.executors.container_support import (
    STAGED_CONTAINER_PYTHON_ROOT,
    container_python_root,
    manifest_referenced_host_paths,
    mount_roots,
)


# A raw-manifest materialization as emitted by the executor for a fastq import.
def _manifest_materialization(input_format="SingleEndFastqManifestPhred33V2"):
    return {
        "mode": "raw",
        "semantic_type": "SampleData[SequencesWithQuality]",
        "input_format": input_format,
        "validate_level": "max",
    }


class ContainerPythonRootTests(unittest.TestCase):
    def test_prefers_repo_src_tree_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            src_root = root / "src"
            package_dir = src_root / "adagio"
            module_file = package_dir / "executors" / "container_support.py"
            work_path = root / "work"

            (package_dir / "executors").mkdir(parents=True)
            work_path.mkdir()
            (package_dir / "__init__.py").write_text("", encoding="utf-8")
            module_file.write_text("", encoding="utf-8")

            result = container_python_root(work_path=work_path, module_file=module_file)

            self.assertEqual(result, src_root)
            self.assertFalse((work_path / STAGED_CONTAINER_PYTHON_ROOT).exists())

    def test_stages_only_adagio_package_from_site_packages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            site_packages = root / "site-packages"
            package_dir = site_packages / "adagio"
            module_file = package_dir / "executors" / "container_support.py"
            work_path = root / "work"

            (package_dir / "executors").mkdir(parents=True)
            (package_dir / "cli").mkdir()
            (site_packages / "psutil").mkdir(parents=True)
            work_path.mkdir()

            (package_dir / "__init__.py").write_text("", encoding="utf-8")
            (package_dir / "cli" / "task_exec.py").write_text(
                "VALUE = 1\n", encoding="utf-8"
            )
            module_file.write_text("", encoding="utf-8")
            (site_packages / "psutil" / "__init__.py").write_text(
                "VALUE = 2\n", encoding="utf-8"
            )

            result = container_python_root(work_path=work_path, module_file=module_file)

            staged_root = work_path / STAGED_CONTAINER_PYTHON_ROOT
            self.assertEqual(result, staged_root)
            self.assertTrue((staged_root / "adagio" / "__init__.py").exists())
            self.assertTrue((staged_root / "adagio" / "cli" / "task_exec.py").exists())
            self.assertFalse((staged_root / "psutil").exists())


# V2 TSV single-end: fastqs live under a different top-level root than the
# manifest file (which the test writes into a tempdir).
_V2_SINGLE = (
    "sample-id\tabsolute-filepath\n"
    "sample1\t/reads/study/sample1.fastq.gz\n"
    "sample2\t/reads/study/sample2.fastq.gz\n"
)
_V2_PAIRED = (
    "sample-id\tforward-absolute-filepath\treverse-absolute-filepath\n"
    "sample1\t/reads/study/s1_R1.fastq.gz\t/reads/study/s1_R2.fastq.gz\n"
)
# V1 CSV, phred64, with a direction column.
_V1_CSV = (
    "sample-id,absolute-filepath,direction\n"
    "sample1,/reads/study/sample1.fastq.gz,forward\n"
    "sample2,/reads/study/sample2.fastq.gz,reverse\n"
)


class ManifestReferencedHostPathsTests(unittest.TestCase):
    def _write_manifest(self, text: str, name: str = "manifest.tsv") -> Path:
        tmp = tempfile.mkdtemp()
        path = Path(tmp) / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_extracts_v2_single_end_interior_paths(self) -> None:
        manifest = self._write_manifest(_V2_SINGLE)
        result = manifest_referenced_host_paths(
            archive_inputs={"data": str(manifest)},
            materializations={"data": _manifest_materialization()},
        )
        self.assertEqual(
            result,
            [
                Path("/reads/study/sample1.fastq.gz"),
                Path("/reads/study/sample2.fastq.gz"),
            ],
        )
        # The referenced fastqs live off a different top-level root than the
        # manifest file, so this is exactly the case the fix exists for.
        self.assertNotEqual(result[0].parts[1], manifest.parts[1])

    def test_extracts_v2_paired_end_interior_paths(self) -> None:
        manifest = self._write_manifest(_V2_PAIRED)
        result = manifest_referenced_host_paths(
            archive_inputs={"data": str(manifest)},
            materializations={
                "data": _manifest_materialization("PairedEndFastqManifestPhred33V2")
            },
        )
        self.assertEqual(
            result,
            [
                Path("/reads/study/s1_R1.fastq.gz"),
                Path("/reads/study/s1_R2.fastq.gz"),
            ],
        )

    def test_extracts_v1_csv_phred64_interior_paths(self) -> None:
        manifest = self._write_manifest(_V1_CSV, name="manifest.csv")
        result = manifest_referenced_host_paths(
            archive_inputs={"data": str(manifest)},
            materializations={
                "data": _manifest_materialization("SingleEndFastqManifestPhred64")
            },
        )
        # The trailing "forward"/"reverse" tokens carry no leading slash, so the
        # scan yields only the fastq paths.
        self.assertEqual(
            result,
            [
                Path("/reads/study/sample1.fastq.gz"),
                Path("/reads/study/sample2.fastq.gz"),
            ],
        )

    def test_folds_to_a_single_foreign_mount_root(self) -> None:
        # End-to-end with mount_roots: many interior paths under one foreign root
        # collapse to that root (when it exists on this host).
        foreign_root = _existing_root_other_than(Path(tempfile.gettempdir()).resolve())
        manifest = self._write_manifest(
            "sample-id\tabsolute-filepath\n"
            f"sample1\t{foreign_root}/adagio-test-reads/s1.fastq.gz\n"
            f"sample2\t{foreign_root}/adagio-test-reads/s2.fastq.gz\n"
        )
        referenced = manifest_referenced_host_paths(
            archive_inputs={"data": str(manifest)},
            materializations={"data": _manifest_materialization()},
        )
        self.assertEqual(mount_roots(referenced), [foreign_root])

    def test_ignores_non_manifest_and_non_raw_materializations(self) -> None:
        manifest = self._write_manifest(_V2_SINGLE)
        archive_inputs = {"data": str(manifest)}
        # Directory import (no input_format), a non-Manifest format, and a
        # non-raw mode all contribute nothing.
        self.assertEqual(
            manifest_referenced_host_paths(
                archive_inputs=archive_inputs,
                materializations={
                    "data": {
                        "mode": "raw",
                        "semantic_type": "X",
                        "validate_level": "max",
                    }
                },
            ),
            [],
        )
        self.assertEqual(
            manifest_referenced_host_paths(
                archive_inputs=archive_inputs,
                materializations={"data": _manifest_materialization("BIOMV210Format")},
            ),
            [],
        )
        self.assertEqual(
            manifest_referenced_host_paths(
                archive_inputs=archive_inputs,
                materializations={
                    "data": {**_manifest_materialization(), "mode": "artifact"}
                },
            ),
            [],
        )

    def test_skips_uri_and_relative_sources(self) -> None:
        self.assertEqual(
            manifest_referenced_host_paths(
                archive_inputs={"data": "s3://bucket/manifest.tsv"},
                materializations={"data": _manifest_materialization()},
            ),
            [],
        )
        self.assertEqual(
            manifest_referenced_host_paths(
                archive_inputs={"data": "relative/manifest.tsv"},
                materializations={"data": _manifest_materialization()},
            ),
            [],
        )

    def test_empty_and_unreadable_inputs_return_empty(self) -> None:
        self.assertEqual(
            manifest_referenced_host_paths(archive_inputs={}, materializations=None),
            [],
        )
        self.assertEqual(
            manifest_referenced_host_paths(
                archive_inputs={"data": "/nonexistent/manifest.tsv"},
                materializations={"data": _manifest_materialization()},
            ),
            [],
        )


def _existing_root_other_than(path: Path) -> Path:
    """A first-level filesystem root that exists and differs from ``path``'s."""
    excluded = path.parts[1] if len(path.parts) > 1 else None
    for candidate in ("usr", "bin", "etc", "opt", "var", "lib"):
        if candidate == excluded:
            continue
        root = Path("/", candidate)
        if root.exists():
            return root
    raise unittest.SkipTest("No foreign top-level root available on this host.")
