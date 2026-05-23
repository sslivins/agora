"""Integration tests for the wired-in call sites of shared.devices_store.

PR 1 of the multi-display work routes credential reads through
``shared.devices_store`` (with legacy ``persist/api_key`` fallback) and
mirrors credential writes into ``persist/devices.json`` slot A.  These
tests exercise the actual call sites in cms_client/service.py and
api/routers/system.py to ensure:

- CMS-pushed ``api_key`` rotation via ``_handle_config`` writes to BOTH
  the legacy ``persist/api_key`` file AND ``persist/devices.json``
  slot A.
- The ``factory_reset`` REST endpoint wipes ``persist/devices.json``
  alongside ``persist/api_key``.
- The CMS-client ``_handle_factory_reset`` does the same.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock heavy deps before importing the service module (mirrors
# test_download_auth_header.py's pattern).
sys.modules.setdefault("websockets", MagicMock())
sys.modules.setdefault("websockets.asyncio", MagicMock())
sys.modules.setdefault("websockets.asyncio.client", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

from cms_client.service import CMSClient  # noqa: E402
from shared.devices_store import (  # noqa: E402
    SLOT_A,
    devices_path,
    read_slot,
)


# ── _handle_config api_key rotation: dual-write ──────────────────────


@pytest.fixture
def cms_client_for_config(tmp_path):
    """Mirror the minimal fixture used by other CMSClient unit tests."""
    settings = MagicMock()
    settings.persist_dir = tmp_path / "persist"
    settings.persist_dir.mkdir()
    settings.state_dir = tmp_path / "state"
    settings.state_dir.mkdir()
    settings.device_name = "agora-node-test"

    with patch.object(CMSClient, "__init__", lambda self, s: None):
        client = CMSClient(settings)
    client.settings = settings
    return client


@pytest.mark.asyncio
async def test_handle_config_api_key_dual_writes_to_devices_json(
    cms_client_for_config, tmp_path
):
    """CMS rotates api_key -> legacy persist/api_key AND devices.json slot A
    both updated."""
    # Stub /boot/agora-config.json -- the handler tries to read+write it
    # but on test hosts /boot/ doesn't exist.  We point at a tmp path
    # via patch.
    fake_boot = tmp_path / "boot_agora_config.json"
    with patch("cms_client.service.Path", side_effect=lambda p: fake_boot if p == "/boot/agora-config.json" else Path(p)):
        await cms_client_for_config._handle_config({"api_key": "rotated-key-123"})

    persist = cms_client_for_config.settings.persist_dir

    # Legacy file: still written.
    assert (persist / "api_key").read_text() == "rotated-key-123"

    # New slot-keyed store: slot A populated with the rotated key, plus
    # device_id derived from settings.device_name.
    slot_a = read_slot(persist, SLOT_A)
    assert slot_a is not None
    assert slot_a["api_key"] == "rotated-key-123"
    assert slot_a["device_id"] == "agora-node-test"


@pytest.mark.asyncio
async def test_handle_config_api_key_preserves_existing_device_id(
    cms_client_for_config, tmp_path
):
    """If devices.json already has a slot-A device_id (e.g. minted earlier),
    a subsequent rotation must not clobber it with settings.device_name."""
    persist = cms_client_for_config.settings.persist_dir
    devices_path(persist).write_text(
        json.dumps({"A": {"device_id": "previously-minted", "api_key": "old"}})
    )

    fake_boot = tmp_path / "boot_agora_config.json"
    with patch("cms_client.service.Path", side_effect=lambda p: fake_boot if p == "/boot/agora-config.json" else Path(p)):
        await cms_client_for_config._handle_config({"api_key": "rotated-key-456"})

    slot_a = read_slot(persist, SLOT_A)
    assert slot_a == {"device_id": "previously-minted", "api_key": "rotated-key-456"}


@pytest.mark.asyncio
async def test_handle_config_no_api_key_leaves_devices_json_alone(
    cms_client_for_config, tmp_path
):
    """A config message without api_key must not touch devices.json."""
    persist = cms_client_for_config.settings.persist_dir
    devices_path(persist).write_text(
        json.dumps({"A": {"device_id": "d1", "api_key": "untouched"}})
    )

    fake_boot = tmp_path / "boot_agora_config.json"
    with patch("cms_client.service.Path", side_effect=lambda p: fake_boot if p == "/boot/agora-config.json" else Path(p)):
        await cms_client_for_config._handle_config({"device_name": "new-name"})

    assert read_slot(persist, SLOT_A) == {"device_id": "d1", "api_key": "untouched"}


# ── factory_reset wipes devices.json ─────────────────────────────────


def test_factory_reset_endpoint_wipes_devices_json(tmp_path, monkeypatch):
    """api/routers/system.py:factory_reset removes persist/devices.json
    alongside persist/api_key."""
    persist = tmp_path / "persist"
    persist.mkdir()
    state = tmp_path / "state"
    state.mkdir()

    # Seed legacy + new credential files.
    (persist / "api_key").write_text("k1")
    devices_path(persist).write_text(
        json.dumps({"A": {"device_id": "d1", "api_key": "k1"}, "B": {"device_id": "d2", "api_key": "k2"}})
    )

    # Patch the module-level PERSIST_DIR/STATE_DIR before importing.
    from api.routers import system as system_mod
    monkeypatch.setattr(system_mod, "PERSIST_DIR", persist)
    monkeypatch.setattr(system_mod, "STATE_DIR", state)

    # Drive the inner cleanup logic directly (skip reboot subprocess
    # and the nmcli shell-out for forget_all_wifi).
    with patch("api.routers.system.subprocess.Popen"), \
         patch("provision.network.forget_all_wifi"):
        settings = MagicMock()
        settings.videos_dir = tmp_path / "videos"
        settings.images_dir = tmp_path / "images"
        settings.slideshows_dir = tmp_path / "slideshows"
        for d in (settings.videos_dir, settings.images_dir, settings.slideshows_dir):
            d.mkdir()
        import asyncio
        asyncio.run(system_mod.factory_reset(settings=settings))

    assert not (persist / "api_key").exists()
    assert not devices_path(persist).exists()


@pytest.mark.asyncio
async def test_cms_client_factory_reset_wipes_devices_json(
    cms_client_for_config, tmp_path
):
    """cms_client/_handle_factory_reset wipes devices.json too."""
    persist = cms_client_for_config.settings.persist_dir
    (persist / "api_key").write_text("k1")
    devices_path(persist).write_text(
        json.dumps({"A": {"device_id": "d1", "api_key": "k1"}})
    )

    # Stub out the parts of _handle_factory_reset that we don't care about
    # for this credential-wipe slice.
    cms_client_for_config.settings.videos_dir = tmp_path / "videos"
    cms_client_for_config.settings.images_dir = tmp_path / "images"
    cms_client_for_config.settings.slideshows_dir = tmp_path / "slideshows"
    for d in (
        cms_client_for_config.settings.videos_dir,
        cms_client_for_config.settings.images_dir,
        cms_client_for_config.settings.slideshows_dir,
    ):
        d.mkdir()

    fake_ws = MagicMock()
    fake_ws.send = AsyncMock()

    with patch("os.system"):  # avoid actual reboot
        await cms_client_for_config._handle_factory_reset(fake_ws)

    assert not (persist / "api_key").exists()
    assert not devices_path(persist).exists()
