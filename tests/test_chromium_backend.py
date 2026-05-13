"""Unit tests for the chromium-shell playback backend (demo).

These tests purposely do not require fastapi/uvicorn to be installed,
and do not spawn cage/chromium or open real network sockets. They cover
the public command API, asset-URL sandboxing, and the buffer-coalescing
behaviour of ``_WebSocketState`` which guards against losing user
intent across shell reconnects.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from player.chromium_backend import ChromiumPlayer, _WebSocketState


# ── Command JSON shape ──────────────────────────────────────────────


@pytest.fixture
def cp(tmp_path):
    """ChromiumPlayer pointed at a temp assets dir; no real subprocess."""
    assets = tmp_path / "assets"
    assets.mkdir()
    return ChromiumPlayer(assets_dir=assets, spawn_chromium=False)


def _capture_commands(cp: ChromiumPlayer) -> list[dict]:
    """Replace ``_enqueue`` with an in-memory sink so we can assert wire shape."""
    sent: list[dict] = []
    cp._enqueue = sent.append  # type: ignore[method-assign]
    return sent


def test_show_image_emits_expected_command(cp, tmp_path):
    sent = _capture_commands(cp)
    img = cp.assets_dir / "images" / "foo.jpg"
    img.parent.mkdir(parents=True)
    img.write_bytes(b"\xff\xd8\xff")

    cp.show_image(img, transition="fade", duration_ms=800)

    assert sent == [{
        "cmd": "show_image",
        "url": "/assets/images/foo.jpg",
        "transition": "fade",
        "duration_ms": 800,
    }]


def test_show_video_passes_loop_and_muted(cp):
    sent = _capture_commands(cp)
    vid = cp.assets_dir / "videos" / "clip.mp4"
    vid.parent.mkdir(parents=True)
    vid.write_bytes(b"\x00\x00\x00\x18ftypmp42")

    cp.show_video(vid, loop=True, muted=True)

    assert len(sent) == 1
    cmd = sent[0]
    assert cmd["cmd"] == "show_video"
    assert cmd["url"] == "/assets/videos/clip.mp4"
    assert cmd["loop"] is True
    assert cmd["muted"] is True


def test_show_splash_uses_no_transition(cp):
    sent = _capture_commands(cp)
    splash = cp.assets_dir / "splash" / "default.png"
    splash.parent.mkdir(parents=True)
    splash.write_bytes(b"\x89PNG\r\n\x1a\n")

    cp.show_splash(splash)

    assert sent == [{
        "cmd": "show_splash",
        "url": "/assets/splash/default.png",
        "transition": "none",
        "duration_ms": 0,
    }]


def test_stop_playback_emits_stop_command(cp):
    sent = _capture_commands(cp)
    cp.stop_playback()
    assert sent == [{"cmd": "stop"}]


# ── Asset-URL sandbox ───────────────────────────────────────────────


def test_asset_url_rejects_paths_outside_assets_dir(cp, tmp_path):
    outside = tmp_path / "evil.jpg"
    outside.write_bytes(b"hi")
    assert cp._asset_url(outside) is None


def test_asset_url_returns_relative_under_assets(cp):
    p = cp.assets_dir / "sub" / "deep" / "file.png"
    p.parent.mkdir(parents=True)
    p.write_bytes(b".")
    assert cp._asset_url(p) == "/assets/sub/deep/file.png"


def test_show_image_outside_sandbox_is_dropped(cp, tmp_path):
    sent = _capture_commands(cp)
    naughty = tmp_path / "naughty.jpg"
    naughty.write_bytes(b".")
    cp.show_image(naughty)
    assert sent == []


# ── _WebSocketState buffering / coalescing ──────────────────────────


class _FakeWebSocket:
    """Minimal ws stand-in: records ``send_text`` calls."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = False

    async def send_text(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        self.closed = True


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_publish_buffers_when_no_socket():
    state = _WebSocketState()
    _run(state.publish({"cmd": "show_image", "url": "/assets/a.jpg"}))
    _run(state.publish({"cmd": "show_image", "url": "/assets/b.jpg"}))
    assert len(state._buffer) == 2


def test_coalesce_keeps_only_latest_show_command():
    state = _WebSocketState()
    state._buffer.append({"cmd": "show_image", "url": "/assets/a.jpg"})
    state._buffer.append({"cmd": "show_image", "url": "/assets/b.jpg"})
    state._buffer.append({"cmd": "show_video", "url": "/assets/c.mp4"})

    out = state._coalesce_buffer()

    assert out == [{"cmd": "show_video", "url": "/assets/c.mp4"}]
    assert not state._buffer  # drain


def test_coalesce_keeps_stop_that_arrived_after_show():
    state = _WebSocketState()
    state._buffer.append({"cmd": "show_image", "url": "/assets/a.jpg"})
    state._buffer.append({"cmd": "stop"})

    out = state._coalesce_buffer()

    assert out == [
        {"cmd": "show_image", "url": "/assets/a.jpg"},
        {"cmd": "stop"},
    ]


def test_coalesce_drops_stop_that_came_before_show():
    state = _WebSocketState()
    state._buffer.append({"cmd": "stop"})
    state._buffer.append({"cmd": "show_image", "url": "/assets/a.jpg"})

    out = state._coalesce_buffer()

    # The show came after, so stop is stale and gets dropped.
    assert out == [{"cmd": "show_image", "url": "/assets/a.jpg"}]


def test_attach_flushes_coalesced_commands():
    state = _WebSocketState()
    state._buffer.append({"cmd": "show_image", "url": "/assets/old.jpg"})
    state._buffer.append({"cmd": "show_video", "url": "/assets/new.mp4"})
    ws = _FakeWebSocket()

    _run(state.attach(ws))

    assert ws.sent == [json.dumps({"cmd": "show_video", "url": "/assets/new.mp4"})]
    assert state._ws is ws


def test_publish_writes_to_attached_socket():
    state = _WebSocketState()
    ws = _FakeWebSocket()

    async def _scenario() -> None:
        await state.attach(ws)
        await state.publish({"cmd": "show_image", "url": "/assets/a.jpg"})

    _run(_scenario())

    assert ws.sent == [json.dumps({"cmd": "show_image", "url": "/assets/a.jpg"})]


def test_detach_clears_current_socket():
    state = _WebSocketState()
    ws = _FakeWebSocket()
    _run(state.attach(ws))
    _run(state.detach(ws))
    assert state._ws is None


# ── Lifecycle without real fastapi/uvicorn ──────────────────────────


def test_is_alive_when_server_thread_absent(cp):
    """``is_alive`` must be False before ``start`` and never raise."""
    assert cp.is_alive() is False


def test_stop_is_idempotent_without_start(cp):
    cp.stop()
    cp.stop()
    # If we got here, no exception was raised.
    assert True


def test_start_without_fastapi_does_not_crash(monkeypatch, cp):
    """If fastapi/uvicorn aren't importable, ``start`` should log and bail
    cleanly rather than crash the player process."""
    import builtins

    real_import = builtins.__import__

    def _no_fastapi(name, *args, **kwargs):
        if name.split(".")[0] in {"fastapi", "uvicorn"}:
            raise ImportError(f"pretend {name} is missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_fastapi)
    # Should not raise; should return promptly because the thread's
    # ready event is set in the ImportError branch.
    cp.start()
    cp.stop()


# ── Routing (regression for /ws being shadowed by StaticFiles) ──────


@pytest.fixture
def routing_cp(tmp_path):
    """ChromiumPlayer with BOTH assets_dir AND shell_dir under tmp_path,
    so route-table tests don't trample the real player/shell/ files."""
    assets = tmp_path / "assets"
    assets.mkdir()
    shell = tmp_path / "shell"
    shell.mkdir()
    (shell / "index.html").write_text("<html>shell-root</html>", encoding="utf-8")
    return ChromiumPlayer(assets_dir=assets, shell_dir=shell, spawn_chromium=False)


def test_ws_route_is_not_shadowed_by_static_mount(routing_cp):
    """Regression: /ws must dispatch to the WebSocket handler, not the
    StaticFiles mount on "/". Previously route order put the catch-all
    static mount first, which made every /ws upgrade hit StaticFiles
    and fail with `assert scope["type"] == "http"`."""
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    client = TestClient(routing_cp._build_app())
    with client.websocket_connect("/ws") as ws:
        # If we got here, the WS handshake completed — i.e. /ws was
        # routed to the websocket endpoint, not into StaticFiles.
        assert ws is not None


def test_assets_mount_is_not_shadowed_by_shell_mount(routing_cp):
    """/assets/<file> must be served from assets_dir, not the shell dir."""
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    (routing_cp.assets_dir / "marker.txt").write_text("from-assets", encoding="utf-8")
    client = TestClient(routing_cp._build_app())
    resp = client.get("/assets/marker.txt")
    assert resp.status_code == 200
    assert resp.text == "from-assets"


def test_root_serves_shell_index(routing_cp):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    client = TestClient(routing_cp._build_app())
    resp = client.get("/")
    assert resp.status_code == 200
    assert "shell-root" in resp.text
