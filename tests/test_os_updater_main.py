"""Tests for the ``/etc/agora/version`` parser in :mod:`os_updater.main`.

agora-os ships ``/etc/agora/version`` as a multi-line key=value file
(``agora_os_version=...``, ``agora_app_floor=...``). The original
implementation did a naive ``.strip()`` of the entire file and treated
the result as the version string, which broke every floor check. These
tests pin the new parser's contract.

Also covers ``_build_transport_factory`` — see TestBuildTransportFactory
at the bottom of the file.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import types

import pytest

from os_updater.main import _read_current_version, _build_transport_factory


def _write(tmp_path, content: str):
    p = tmp_path / "version"
    p.write_text(content, encoding="utf-8")
    return p


class TestReadCurrentVersion:
    def test_returns_agora_os_version_value(self, tmp_path):
        p = _write(
            tmp_path,
            "# header\nagora_os_version=0.0.4-test\nagora_app_floor=1.11.0\n",
        )
        assert _read_current_version(p) == "0.0.4-test"

    def test_skips_comments_and_blank_lines(self, tmp_path):
        p = _write(
            tmp_path,
            "\n# comment line\n\nagora_os_version=1.2.3\n\n# trailing comment\n",
        )
        assert _read_current_version(p) == "1.2.3"

    def test_tolerates_unknown_keys(self, tmp_path):
        # agora-os may grow new keys without coordinating with this
        # parser; only the missing agora_os_version line is fatal.
        p = _write(
            tmp_path,
            "agora_os_version=2.0.0\nagora_app_floor=1.11.0\nfuture_field=hello\n",
        )
        assert _read_current_version(p) == "2.0.0"

    def test_strips_surrounding_whitespace_in_value(self, tmp_path):
        p = _write(tmp_path, "agora_os_version =   1.2.3   \n")
        assert _read_current_version(p) == "1.2.3"

    def test_missing_agora_os_version_raises(self, tmp_path):
        p = _write(tmp_path, "agora_app_floor=1.11.0\nfuture_field=hello\n")
        with pytest.raises(RuntimeError, match="agora_os_version"):
            _read_current_version(p)

    def test_empty_value_raises(self, tmp_path):
        p = _write(tmp_path, "agora_os_version=\n")
        with pytest.raises(RuntimeError, match="empty value"):
            _read_current_version(p)

    def test_empty_file_raises(self, tmp_path):
        p = _write(tmp_path, "")
        with pytest.raises(RuntimeError, match="agora_os_version"):
            _read_current_version(p)

    def test_comment_only_file_raises(self, tmp_path):
        p = _write(tmp_path, "# only comments\n# nothing useful\n")
        with pytest.raises(RuntimeError, match="agora_os_version"):
            _read_current_version(p)

    def test_malformed_line_without_equals_raises(self, tmp_path):
        p = _write(
            tmp_path,
            "agora_os_version=1.0.0\nthis-line-has-no-equals\n",
        )
        with pytest.raises(RuntimeError, match="malformed line"):
            _read_current_version(p)

    def test_literal_todo_placeholder_passes_through(self, tmp_path):
        # Regression: pre-fix v0.0.4 images shipped with literal "TODO"
        # as the value. The parser shouldn't blow up on it — the floor
        # check downstream is responsible for noticing "TODO" isn't
        # semver-shaped. This test pins that we return the literal
        # string rather than swallowing it as "missing".
        p = _write(tmp_path, "agora_os_version=TODO\nagora_app_floor=TODO\n")
        assert _read_current_version(p) == "TODO"

    def test_multiple_agora_os_version_lines_last_wins(self, tmp_path):
        # Not a documented format feature, but the parser walks the
        # whole file; pin "last write wins" so a future bug that adds
        # duplicate lines is at least deterministic.
        p = _write(
            tmp_path,
            "agora_os_version=1.0.0\nagora_os_version=2.0.0\n",
        )
        assert _read_current_version(p) == "2.0.0"


# ---------------------------------------------------------------------------
# _build_transport_factory — dual-path (bootstrap-v2 / legacy api_key)
# ---------------------------------------------------------------------------


class _CapturedOpenWPS:
    """Async stand-in for ``cms_client.transport.open_wps``.

    Records the kwargs it was called with so tests can assert the
    factory routed through the right path.
    """

    def __init__(self):
        self.calls: list[dict] = []

    async def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(
            send=lambda *_a, **_kw: None,
            recv=lambda *_a, **_kw: None,
            close=lambda *_a, **_kw: None,
        )


def _stub_settings(tmp_path, *, bootstrap_v2: bool, **overrides):
    """Minimal duck-typed Settings stand-in for these tests.

    Avoids pulling in api.config.Settings full validation surface, which
    would force these tests to bake in unrelated fields.
    """
    s = types.SimpleNamespace(
        bootstrap_v2=bootstrap_v2,
        bootstrap_state_path=tmp_path / "bootstrap_state.json",
        cms_url="wss://cms.example/ws/device",
        device_name="legacy-device-name",
        device_api_key="legacy-api-key",
        cms_api_url="https://cms.example",
    )
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _write_bootstrap_state(path, **fields):
    base = {
        "wps_url": "wss://wps.example/client/hubs/agora?access_token=AAA",
        "wps_jwt": "AAA",
        "cms_api_base": "https://cms.example",
        "device_id": "v2-device-uuid",
    }
    base.update(fields)
    path.write_text(json.dumps(base))


class TestBuildTransportFactory:
    """Pins the bootstrap-v2 / legacy api_key dual-path contract.

    The factory itself constructs the connect closure but does not
    invoke ``open_wps``; the closure runs only when the service
    asks for a fresh transport. Tests below exercise both halves —
    factory-build time (validation, error messages) and connect time
    (which kwargs the closure passes through).
    """

    def test_bootstrap_v2_routes_through_pre_minted_kwargs(self, tmp_path, monkeypatch):
        captured = _CapturedOpenWPS()
        monkeypatch.setattr("cms_client.transport.open_wps", captured)
        _write_bootstrap_state(tmp_path / "bootstrap_state.json")
        settings = _stub_settings(tmp_path, bootstrap_v2=True)

        factory = _build_transport_factory(settings)
        # Drive the opener closure directly — the adapter's full
        # connect() path adds an async-iterator dance we don't need
        # to mock to assert routing.
        asyncio.run(factory()._opener())

        assert len(captured.calls) == 1
        call = captured.calls[0]
        assert call["pre_minted_url"] == "wss://wps.example/client/hubs/agora?access_token=AAA"
        assert call["pre_minted_token"] == "AAA"
        assert call["cms_url"] == "https://cms.example"
        assert call["device_id"] == "v2-device-uuid"
        assert "api_key" not in call

    def test_bootstrap_v2_rereads_state_on_each_connect(self, tmp_path, monkeypatch):
        # cms_client refreshes bootstrap_state.json out-of-band; the
        # factory must pick up the new JWT on the next reconnect rather
        # than caching the first read.
        captured = _CapturedOpenWPS()
        monkeypatch.setattr("cms_client.transport.open_wps", captured)
        state_path = tmp_path / "bootstrap_state.json"
        _write_bootstrap_state(state_path, wps_jwt="JWT-1")
        settings = _stub_settings(tmp_path, bootstrap_v2=True)

        factory = _build_transport_factory(settings)

        asyncio.run(factory()._opener())
        _write_bootstrap_state(state_path, wps_jwt="JWT-2")
        asyncio.run(factory()._opener())

        assert [c["pre_minted_token"] for c in captured.calls] == ["JWT-1", "JWT-2"]

    def test_bootstrap_v2_missing_state_falls_back_to_legacy(self, tmp_path, monkeypatch):
        # Fresh-flash race: bootstrap-v2 enabled in settings but the
        # device hasn't completed enrollment yet. Rather than crashing
        # the os-updater service, fall through to the legacy path
        # (which will then surface its own clearer error if env vars
        # are also missing).
        captured = _CapturedOpenWPS()
        monkeypatch.setattr("cms_client.transport.open_wps", captured)
        # state file absent
        settings = _stub_settings(tmp_path, bootstrap_v2=True)

        factory = _build_transport_factory(settings)
        asyncio.run(factory()._opener())

        assert captured.calls[0]["api_key"] == "legacy-api-key"
        assert "pre_minted_url" not in captured.calls[0]

    def test_bootstrap_v2_state_missing_keys_falls_back_to_legacy(self, tmp_path, monkeypatch):
        # Partially populated state file (e.g. cms_client mid-write,
        # or an older state format that pre-dates wps_url). Same fallback
        # rule as the missing-file case.
        captured = _CapturedOpenWPS()
        monkeypatch.setattr("cms_client.transport.open_wps", captured)
        state_path = tmp_path / "bootstrap_state.json"
        state_path.write_text(json.dumps({"wps_url": "wss://x", "wps_jwt": ""}))
        settings = _stub_settings(tmp_path, bootstrap_v2=True)

        factory = _build_transport_factory(settings)
        asyncio.run(factory()._opener())

        assert captured.calls[0]["api_key"] == "legacy-api-key"

    def test_legacy_path_when_flag_off_even_if_state_present(self, tmp_path, monkeypatch):
        # Defensive: a stale bootstrap_state.json shouldn't accidentally
        # flip a legacy device onto the v2 path. The settings flag is
        # the source of truth.
        captured = _CapturedOpenWPS()
        monkeypatch.setattr("cms_client.transport.open_wps", captured)
        _write_bootstrap_state(tmp_path / "bootstrap_state.json")
        settings = _stub_settings(tmp_path, bootstrap_v2=False)

        factory = _build_transport_factory(settings)
        asyncio.run(factory()._opener())

        assert captured.calls[0]["api_key"] == "legacy-api-key"
        assert "pre_minted_url" not in captured.calls[0]

    @pytest.mark.parametrize(
        "missing_field,expected_msg",
        [
            ("cms_url", "AGORA_CMS_URL"),
            ("device_name", "AGORA_DEVICE_NAME"),
            ("device_api_key", "AGORA_DEVICE_API_KEY"),
        ],
    )
    def test_legacy_path_raises_on_missing_env(self, tmp_path, missing_field, expected_msg):
        settings = _stub_settings(tmp_path, bootstrap_v2=False)
        setattr(settings, missing_field, "")
        with pytest.raises(RuntimeError, match=expected_msg):
            _build_transport_factory(settings)


# ---------------------------------------------------------------------------
# _run — wires production collaborators into OSUpdaterService
# ---------------------------------------------------------------------------


class TestServiceWiring:
    """Pins that ``_run`` constructs ``OSUpdaterService`` with the real
    ``BundleDownloader``/``SignatureVerifier``/``SlotStager`` instances.

    The pre-M7 bug was that ``main.py`` constructed the service without
    those kwargs, leaving the ``_Default*`` stubs in place; the first
    dispatch then died with ``NotImplementedError`` deep inside the FSM.
    This test guards against that regression by intercepting the
    ``OSUpdaterService.__init__`` call and inspecting the captured
    kwargs.
    """

    def _drive_run(self, monkeypatch, tmp_path):
        from os_updater import main as main_mod
        from os_updater.apply import SlotStager
        from os_updater.downloader import BundleDownloader
        from os_updater.verifier import SignatureVerifier

        captured: dict = {}

        class _FakeService:
            def __init__(self, **kwargs):
                captured["kwargs"] = kwargs

            async def run(self):
                # Return immediately so the wait() in _run resolves on
                # the runner side, no need to fire the stop event.
                return None

        monkeypatch.setattr(main_mod, "OSUpdaterService", _FakeService)
        monkeypatch.setattr(
            main_mod,
            "_read_current_version",
            lambda _p: "0.0.22-test",
        )
        monkeypatch.setattr(
            main_mod,
            "_build_transport_factory",
            lambda _settings: (lambda: None),
        )

        # api.config.load_settings is lazy-imported inside _run; install
        # a stand-in into sys.modules so the import resolves to it.
        fake_api_config = types.ModuleType("api.config")
        fake_api_config.load_settings = lambda: _stub_settings(
            tmp_path, bootstrap_v2=True
        )
        monkeypatch.setitem(sys.modules, "api.config", fake_api_config)

        args = argparse.Namespace(
            state_path=tmp_path / "updater-state.json",
            staging_root=tmp_path / "staging",
            current_version_file=tmp_path / "version",
        )
        asyncio.run(main_mod._run(args))

        return captured["kwargs"], BundleDownloader, SignatureVerifier, SlotStager

    def test_wires_real_downloader_verifier_stager(self, tmp_path, monkeypatch):
        kwargs, BundleDownloader, SignatureVerifier, SlotStager = self._drive_run(
            monkeypatch, tmp_path
        )

        assert isinstance(kwargs["downloader"], BundleDownloader)
        assert isinstance(kwargs["verifier"], SignatureVerifier)
        assert isinstance(kwargs["stager"], SlotStager)

    def test_passes_state_path_and_staging_root_from_args(self, tmp_path, monkeypatch):
        kwargs, *_ = self._drive_run(monkeypatch, tmp_path)

        assert kwargs["state_path"] == tmp_path / "updater-state.json"
        assert kwargs["staging_root"] == tmp_path / "staging"

    def test_current_version_provider_returns_parsed_version(self, tmp_path, monkeypatch):
        kwargs, *_ = self._drive_run(monkeypatch, tmp_path)

        assert kwargs["current_version_provider"]() == "0.0.22-test"

    def test_transport_factory_is_passed_through(self, tmp_path, monkeypatch):
        kwargs, *_ = self._drive_run(monkeypatch, tmp_path)

        # _build_transport_factory was stubbed to return a callable.
        assert callable(kwargs["transport_factory"])
