"""Worker-side staging of HTTPS artifact and metadata inputs, independent of the executor.

The directory is shared by consumers within one run, never between runs. A
completed download is published only after checksum and input validation.
"""

from contextlib import contextmanager
from hashlib import sha256
from http.client import IncompleteRead
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
from typing import Callable
from urllib.error import HTTPError
from urllib.parse import urldefrag, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


CHUNK_SIZE = 1024 * 1024
MAX_ATTEMPTS = 3
TIMEOUT_SECONDS = 60


def parse_artifact_url(source: str) -> tuple[str, str | None]:
    url, fragment = urldefrag(source)
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname:
        raise ValueError("Remote inputs require an HTTPS URL.")
    if parts.username is not None or parts.password is not None:
        raise ValueError("Remote inputs do not support URL credentials.")
    expected = None
    if fragment:
        if not re.fullmatch(r"sha256=[0-9a-fA-F]{64}", fragment):
            raise ValueError(
                "A URL fragment must be #sha256=<64 hex digits>."
            )
        expected = fragment.removeprefix("sha256=").lower()
    return url, expected


def display_url(url: str) -> str:
    """Keep query credentials out of logs and provenance records."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


class _HTTPSRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parse_artifact_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


@contextmanager
def _file_lock(path: Path):
    # OS locks are released if a task is killed, unlike lock directories. Both
    # Docker/Apptainer and Conda consumers use the same run-scoped directory.
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt

            handle.write(b"\0")
            handle.flush()
            handle.seek(0)
            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    time.sleep(0.1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class LocalWriteError(RuntimeError):
    """Disk failures are not network failures: never retried."""


@contextmanager
def _local_write(destination: Path, label: str):
    try:
        yield
    except OSError as exc:
        raise LocalWriteError(
            f"Cannot save input from {label} in {destination.parent}: "
            f"{exc.strerror or type(exc).__name__}."
        ) from None


def _check_free_space(directory: Path, size: int, label: str) -> None:
    free = shutil.disk_usage(directory).free
    if size > free:
        raise LocalWriteError(
            f"Not enough disk space for input from {label}: it is {size} bytes "
            f"and {directory} has {free} bytes free."
        )


class RemoteArtifactStore:
    def __init__(self, directory: Path):
        self.directory = directory

    def resolve(
        self,
        source: str,
        *,
        validate: Callable[[str], dict[str, str]],
    ) -> tuple[str, dict]:
        url, expected = parse_artifact_url(source)
        key = sha256(url.encode()).hexdigest()
        self.directory.mkdir(parents=True, exist_ok=True)
        destination = self.directory / f"{key}.qza"
        receipt = self.directory / f"{key}.json"
        with _file_lock(self.directory / f"{key}.lock"):
            if receipt.is_file():
                record = json.loads(receipt.read_text())
                suffix = ".tsv" if record.get("type") == "Metadata" else ".qza"
                destination = self.directory / f"{key}{suffix}"
                if destination.is_file():
                    self._check_checksum(record["sha256"], expected)
                    # A URL may be consumed as either metadata or an artifact.
                    # Check the current consumer's contract even on cache hits.
                    validate(str(destination))
                    return str(destination), record

            receipt.unlink(missing_ok=True)
            fd, temporary = tempfile.mkstemp(dir=self.directory, suffix=".partial")
            os.close(fd)
            receipt_tmp = self.directory / f"{key}.json.partial"
            try:
                record = self._download(url, Path(temporary))
                self._check_checksum(record["sha256"], expected)
                # The consumer validates artifacts or metadata before publish.
                record.update(validate(temporary))
                suffix = ".tsv" if record.get("type") == "Metadata" else ".qza"
                destination = self.directory / f"{key}{suffix}"
                record["source_id"] = key
                Path(temporary).replace(destination)
                # Under the lock, the receipt is the commit marker. A crashed
                # writer without a complete receipt is downloaded again.
                receipt_tmp.write_text(json.dumps(record), encoding="utf-8")
                receipt_tmp.replace(receipt)
                return str(destination), record
            finally:
                Path(temporary).unlink(missing_ok=True)
                receipt_tmp.unlink(missing_ok=True)

    @staticmethod
    def _check_checksum(observed: str, expected: str | None) -> None:
        if expected is not None and observed != expected:
            raise ValueError(
                f"Remote input SHA-256 mismatch: expected {expected}, got {observed}."
            )

    def _download(self, url: str, destination: Path) -> dict:
        label = display_url(url)
        opener = build_opener(_HTTPSRedirectHandler())
        for attempt in range(MAX_ATTEMPTS):
            print(f"Downloading input: {label} (attempt {attempt + 1})", flush=True)
            try:
                request = Request(url, headers={"User-Agent": "adagio-cli"})
                with opener.open(request, timeout=TIMEOUT_SECONDS) as response:
                    parse_artifact_url(response.geturl())
                    expected_size = response.headers.get("Content-Length")
                    if expected_size is not None:
                        _check_free_space(destination.parent, int(expected_size), label)
                    digest = sha256()
                    size = 0
                    last_progress = time.monotonic()
                    # Only file operations are local; response.read stays retryable.
                    with _local_write(destination, label):
                        handle = destination.open("wb")
                    with handle:
                        while chunk := response.read(CHUNK_SIZE):
                            with _local_write(destination, label):
                                handle.write(chunk)
                            digest.update(chunk)
                            size += len(chunk)
                            if time.monotonic() - last_progress >= 10:
                                print(f"Downloaded {size} bytes: {label}", flush=True)
                                last_progress = time.monotonic()
                        with _local_write(destination, label):
                            handle.flush()
                    if expected_size is not None and size != int(expected_size):
                        raise IncompleteRead(b"", int(expected_size) - size)
                    print(f"Downloaded {size} bytes: {label}", flush=True)
                    return {
                        "url": label,
                        "resolved_url": display_url(response.geturl()),
                        "sha256": digest.hexdigest(),
                        "bytes": size,
                    }
            # OSError covers URLError, timeouts, resets and TLS failures mid-body.
            except (OSError, IncompleteRead) as exc:
                retryable = not isinstance(exc, HTTPError) or exc.code in {
                    408,
                    429,
                    500,
                    502,
                    503,
                    504,
                }
                if retryable and attempt + 1 < MAX_ATTEMPTS:
                    time.sleep(attempt + 1)
                    continue
                detail = (
                    f"HTTP {exc.code}"
                    if isinstance(exc, HTTPError)
                    else type(exc).__name__
                )
                raise RuntimeError(
                    f"Cannot download input from {label}: {detail}."
                ) from None
        raise AssertionError("Unreachable download attempt")
