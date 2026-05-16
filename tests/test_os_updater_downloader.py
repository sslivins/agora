"""Tests for :mod:`os_updater.downloader`.

Exercises the streaming HTTP fetch path that wires the real
``BundleDownloader`` into the OS updater service (M7 stub-stack fix).
``aiohttp`` is mocked at ``sys.modules`` so no network or real
``ClientSession`` is involved.

The downloader's ``except aiohttp.ClientError`` branch (downloader.py:156)
catches whatever ``aiohttp.ClientError`` resolves to *at call time*. A
bare ``MagicMock()`` for the ``aiohttp`` module would make
``ClientError`` itself a MagicMock — and Python refuses to ``except`` a
non-BaseException-derived value at runtime. We override
``aiohttp.ClientError`` with a real exception class so the catch arm
is exercisable.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── aiohttp shim ───────────────────────────────────────────────────────────


class _FakeClientError(Exception):
    """Stand-in for ``aiohttp.ClientError`` used by ``downloader._fetch``.

    Must be a real Exception subclass — Python refuses to catch a
    MagicMock at runtime (``TypeError: catching classes that do not
    inherit from BaseException``).
    """


_aiohttp_stub = MagicMock()
_aiohttp_stub.ClientError = _FakeClientError
sys.modules.setdefault("aiohttp", _aiohttp_stub)


from os_updater.bundle import BundleError  # noqa: E402
from os_updater.dispatch import DispatchPayload  # noqa: E402
from os_updater.downloader import (  # noqa: E402
    DEFAULT_CHUNK_SIZE,
    BundleDownloader,
    BundleDownloadError,
)
from os_updater.verifier import (  # noqa: E402
    DEFAULT_BUNDLE_FILENAME,
    DEFAULT_SIGNATURE_FILENAME,
)


# ── aiohttp mock helpers (mirrors test_download_auth_header.py) ────────────


class _AsyncIterChunks:
    """Async iterator that yields a fixed list of byte chunks."""

    def __init__(self, chunks):
        self._chunks = list(chunks)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._chunks:
            return self._chunks.pop(0)
        raise StopAsyncIteration


def _mock_aiohttp_session(
    *, status: int = 200, body_chunks=(b"payload",), session_get_raises=None
):
    """Build a (mock_aiohttp_module, mock_cls, mock_session) triple.

    ``status`` controls the HTTP status returned by the mocked response.
    ``body_chunks`` is the sequence of byte chunks the response yields
    via ``iter_chunked``. ``session_get_raises``, if set, makes
    ``session.get(...)`` raise that exception instance directly (used
    to simulate ``aiohttp.ClientError``).
    """

    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.content.iter_chunked.return_value = _AsyncIterChunks(body_chunks)

    mock_session = MagicMock()
    if session_get_raises is not None:
        mock_session.get.side_effect = session_get_raises
    else:
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_cls = MagicMock()
    mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_aiohttp = MagicMock()
    mock_aiohttp.ClientSession = mock_cls
    mock_aiohttp.ClientError = _FakeClientError
    return mock_aiohttp, mock_cls, mock_session


def _payload(**overrides) -> DispatchPayload:
    base = dict(
        release_id="rel-1",
        target_version="0.0.23",
        min_from_version="0.0.0",
        bundle_url="https://example.com/bundle.tar.zst",
        signature_url="https://example.com/bundle.tar.zst.minisig",
    )
    base.update(overrides)
    return DispatchPayload(**base)


# ── BundleDownloadError shape ──────────────────────────────────────────────


class TestBundleDownloadError:
    def test_is_bundle_error_subclass(self):
        assert issubclass(BundleDownloadError, BundleError)


# ── BundleDownloader happy path ────────────────────────────────────────────


class TestBundleDownloaderHappyPath:
    def test_writes_both_artifacts(self, tmp_path):
        staging = tmp_path / "stage"
        downloader = BundleDownloader()

        # Two separate sessions are opened (one per artifact). The
        # mock_cls returns a fresh context-manager wrapper on every
        # call, so reusing the same mock is fine -- but we want each
        # fetch to see its own body. Easiest: build a side_effect on
        # the response iter_chunked that flips between bundle/sig.
        mock_aiohttp, mock_cls, mock_session = _mock_aiohttp_session(
            body_chunks=[b"bundle-bytes"]
        )

        # Second session.get() call (for signature) reuses same mocked
        # response object; iter_chunked returns a new iterator each
        # time it's called.
        bodies = iter([
            _AsyncIterChunks([b"bundle-bytes"]),
            _AsyncIterChunks([b"sig-bytes"]),
        ])
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.content.iter_chunked.side_effect = lambda _n: next(bodies)
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            asyncio.run(downloader.run(_payload(), staging))

        bundle_path = staging / DEFAULT_BUNDLE_FILENAME
        sig_path = staging / DEFAULT_SIGNATURE_FILENAME
        assert bundle_path.read_bytes() == b"bundle-bytes"
        assert sig_path.read_bytes() == b"sig-bytes"
        # No leftover .tmp siblings.
        assert not any(p.name.endswith(".tmp") for p in staging.iterdir())

    def test_creates_staging_dir_when_missing(self, tmp_path):
        staging = tmp_path / "does-not-exist-yet" / "deeper"
        assert not staging.exists()

        downloader = BundleDownloader()
        mock_aiohttp, _, mock_session = _mock_aiohttp_session()
        bodies = iter([
            _AsyncIterChunks([b"a"]),
            _AsyncIterChunks([b"b"]),
        ])
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.content.iter_chunked.side_effect = lambda _n: next(bodies)
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            asyncio.run(downloader.run(_payload(), staging))

        assert staging.is_dir()
        assert (staging / DEFAULT_BUNDLE_FILENAME).exists()
        assert (staging / DEFAULT_SIGNATURE_FILENAME).exists()

    def test_custom_filenames_honored(self, tmp_path):
        staging = tmp_path / "stage"
        downloader = BundleDownloader(
            bundle_filename="custom.tar.zst",
            signature_filename="custom.minisig",
        )

        mock_aiohttp, _, mock_session = _mock_aiohttp_session()
        bodies = iter([
            _AsyncIterChunks([b"b"]),
            _AsyncIterChunks([b"s"]),
        ])
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.content.iter_chunked.side_effect = lambda _n: next(bodies)
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            asyncio.run(downloader.run(_payload(), staging))

        assert (staging / "custom.tar.zst").exists()
        assert (staging / "custom.minisig").exists()
        # Default-named files must NOT have been created.
        assert not (staging / DEFAULT_BUNDLE_FILENAME).exists()
        assert not (staging / DEFAULT_SIGNATURE_FILENAME).exists()

    def test_chunk_size_passed_to_iter_chunked(self, tmp_path):
        staging = tmp_path / "stage"
        downloader = BundleDownloader(chunk_size=4096)

        seen_chunk_sizes = []

        def _record(n):
            seen_chunk_sizes.append(n)
            return _AsyncIterChunks([b"x"])

        mock_aiohttp, _, mock_session = _mock_aiohttp_session()
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.content.iter_chunked.side_effect = _record
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            asyncio.run(downloader.run(_payload(), staging))

        # Two fetches (bundle + signature), both with the override.
        assert seen_chunk_sizes == [4096, 4096]

    def test_default_chunk_size_constant(self):
        assert DEFAULT_CHUNK_SIZE == 65536
        assert BundleDownloader().chunk_size == DEFAULT_CHUNK_SIZE


# ── BundleDownloader failure modes ─────────────────────────────────────────


class TestBundleDownloaderFailures:
    def test_http_404_raises_and_cleans_tmp(self, tmp_path):
        staging = tmp_path / "stage"
        downloader = BundleDownloader()
        mock_aiohttp, _, _ = _mock_aiohttp_session(status=404, body_chunks=())

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            with pytest.raises(BundleDownloadError) as exc_info:
                asyncio.run(downloader.run(_payload(), staging))

        assert "404" in str(exc_info.value)
        # The staging dir exists (we mkdir'd it) but must contain
        # neither a .tmp nor a fully-written artifact.
        if staging.exists():
            assert not any(p.name.endswith(".tmp") for p in staging.iterdir())
            assert not (staging / DEFAULT_BUNDLE_FILENAME).exists()

    def test_http_500_raises_and_cleans_tmp(self, tmp_path):
        staging = tmp_path / "stage"
        downloader = BundleDownloader()
        mock_aiohttp, _, _ = _mock_aiohttp_session(status=500, body_chunks=())

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            with pytest.raises(BundleDownloadError) as exc_info:
                asyncio.run(downloader.run(_payload(), staging))

        assert "500" in str(exc_info.value)
        if staging.exists():
            assert not any(p.name.endswith(".tmp") for p in staging.iterdir())

    def test_aiohttp_client_error_wrapped(self, tmp_path):
        """A network-level ``aiohttp.ClientError`` must surface as a
        ``BundleDownloadError`` (with the original exception chained
        via ``__cause__``) so the service can classify it.
        """
        staging = tmp_path / "stage"
        downloader = BundleDownloader()

        original = _FakeClientError("connection reset")
        mock_aiohttp, _, _ = _mock_aiohttp_session(session_get_raises=original)

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            with pytest.raises(BundleDownloadError) as exc_info:
                asyncio.run(downloader.run(_payload(), staging))

        assert exc_info.value.__cause__ is original
        assert "network error" in str(exc_info.value).lower()
        if staging.exists():
            assert not any(p.name.endswith(".tmp") for p in staging.iterdir())

    def test_local_write_error_wrapped(self, tmp_path, monkeypatch):
        """An ``OSError`` during the write path -- e.g. disk full -- must
        surface as a ``BundleDownloadError`` so the service classifies
        it as ``download_failed`` rather than letting a raw OSError
        escape and collapse to ``error_OSError``.
        """
        staging = tmp_path / "stage"
        downloader = BundleDownloader()

        mock_aiohttp, _, mock_session = _mock_aiohttp_session(
            body_chunks=[b"payload"]
        )
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.content.iter_chunked.return_value = _AsyncIterChunks([b"payload"])
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)

        real_open = open

        def _open_explodes(path, *args, **kwargs):
            p = str(path)
            if p.endswith(".tmp"):
                raise OSError(28, "No space left on device")
            return real_open(path, *args, **kwargs)

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            with patch("builtins.open", side_effect=_open_explodes):
                with pytest.raises(BundleDownloadError) as exc_info:
                    asyncio.run(downloader.run(_payload(), staging))

        assert "local write" in str(exc_info.value).lower()
        assert isinstance(exc_info.value.__cause__, OSError)
        if staging.exists():
            assert not any(p.name.endswith(".tmp") for p in staging.iterdir())
