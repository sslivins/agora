"""Tests for the player-side slideshow sequencer (Commit 5c).

Covers:
- Slideshow manifest reading (good / missing / invalid).
- apply_desired() with asset_type=slideshow routes to _start_slideshow
  instead of single-asset resolution.
- _play_next_slide() advances and loops, honouring slideshow-level
  loop_count.
- Mid-flight transition out of a slideshow cancels the slide timeout
  and clears state.
- mpv exit during a play_to_end video slide triggers next-slide advance.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shared.models import DesiredState, PlaybackMode


@pytest.fixture
def mpv_player(tmp_path):
    """An AgoraPlayer wired up enough for slideshow sequencer tests."""
    with patch.dict("sys.modules", {
        "gi": MagicMock(),
        "gi.repository": MagicMock(),
    }):
        import importlib
        import player.service as svc
        importlib.reload(svc)

        p = svc.AgoraPlayer.__new__(svc.AgoraPlayer)
        p.pipeline = None
        p._mpv_process = None
        p._sway_process = None
        p._sway_scope_unit = None
        p.current_desired = None
        p._plymouth_quit = False
        p._current_path = None
        p._current_mtime = None
        p._health_retries = 0
        p._error_retry_delay = 3
        p._pending_error = None
        p._loops_completed = 0
        p._board = svc.Board.PI_5
        p._player_backend = "mpv"
        p._slideshow = None
        p.assets_dir = tmp_path / "assets"
        p.assets_dir.mkdir()
        (p.assets_dir / "slideshows").mkdir()
        (p.assets_dir / "images").mkdir()
        (p.assets_dir / "videos").mkdir()
        p.desired_path = tmp_path / "desired.json"
        p.splash_config_path = tmp_path / "splash"
        # Test scaffolding for what the sequencer touches.
        p._update_current = MagicMock()
        p._show_splash = MagicMock()
        p._start_mpv = MagicMock()
        p._loadfile_mpv = MagicMock(return_value=True)
        yield p, svc


def _write_manifest(player, name, slides):
    path = player.assets_dir / "slideshows" / f"{name}.json"
    import json
    path.write_text(json.dumps({"name": name, "slides": slides}))
    return path


class TestManifestRead:
    def test_returns_dict_when_valid(self, mpv_player):
        player, _ = mpv_player
        _write_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image", "duration_ms": 1000},
        ])
        result = player._read_slideshow_manifest("Show")
        assert result is not None
        m, digest = result
        assert m["name"] == "Show"
        assert len(m["slides"]) == 1
        assert isinstance(digest, str) and len(digest) == 64

    def test_returns_none_when_missing(self, mpv_player):
        player, _ = mpv_player
        assert player._read_slideshow_manifest("Nope") is None

    def test_returns_none_for_empty_slides(self, mpv_player):
        player, _ = mpv_player
        _write_manifest(player, "Show", [])
        assert player._read_slideshow_manifest("Show") is None

    def test_returns_none_for_malformed_json(self, mpv_player):
        player, _ = mpv_player
        path = player.assets_dir / "slideshows" / "Show.json"
        path.write_text("{not json")
        assert player._read_slideshow_manifest("Show") is None


class TestStartSlideshow:
    def test_missing_manifest_falls_back_to_splash(self, mpv_player):
        player, _ = mpv_player
        player._start_slideshow("Nope", None)
        player._show_splash.assert_called_once()
        assert player._slideshow is None

    def test_valid_manifest_kicks_off_first_slide(self, mpv_player):
        player, svc = mpv_player
        # Image slide → _loadfile_mpv path + GLib timeout
        (player.assets_dir / "images" / "a.png").touch()
        _write_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 5000, "play_to_end": False},
        ])
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 42
            player._start_slideshow("Show", None)
        assert player._slideshow is not None
        assert player._slideshow["name"] == "Show"
        # Index advanced past slide 0
        assert player._slideshow["index"] == 1
        # Image slide → IPC loadfile + timeout for duration_ms
        player._loadfile_mpv.assert_called_once()
        glib.timeout_add.assert_called_once()
        ms_arg, _cb = glib.timeout_add.call_args[0]
        assert ms_arg == 5000
        assert player._slideshow["timeout_id"] == 42


class TestManifestSchemaVersion:
    """Phase 0 of agora#226: parse manifest_schema_version, default to 1.0,
    warn-once on forward versions, store on runtime state for Phase 2.
    """

    def _write_versioned_manifest(self, player, name, slides, version):
        import json
        path = player.assets_dir / "slideshows" / f"{name}.json"
        payload = {"name": name, "slides": slides}
        if version is not None:
            payload["manifest_schema_version"] = version
        path.write_text(json.dumps(payload))
        return path

    def test_v1_0_default_when_field_absent(self, mpv_player):
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        self._write_versioned_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 1000, "play_to_end": False},
        ], version=None)
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        assert player._slideshow["schema_version"] == "1.0"

    def test_v1_0_explicit(self, mpv_player):
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        self._write_versioned_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 1000, "play_to_end": False},
        ], version="1.0")
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        assert player._slideshow["schema_version"] == "1.0"

    def test_forward_version_logs_info_and_still_plays(self, mpv_player, caplog):
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        self._write_versioned_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 1000, "play_to_end": False},
        ], version="1.2")
        with patch.object(svc, "GLib"), caplog.at_level("INFO", logger="agora.player"):
            player._start_slideshow("Show", None)
        assert player._slideshow is not None
        assert player._slideshow["schema_version"] == "1.2"
        # INFO logged, mentions both the observed and the player-max version.
        assert any(
            "manifest_schema_version=1.2" in rec.getMessage()
            and "player max=1.1" in rec.getMessage()
            for rec in caplog.records
        ), f"forward-version INFO not logged: {[r.getMessage() for r in caplog.records]}"

    def test_forward_version_logged_only_once_per_digest(self, mpv_player, caplog):
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        self._write_versioned_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 1000, "play_to_end": False},
        ], version="1.2")
        with patch.object(svc, "GLib"), caplog.at_level("INFO", logger="agora.player"):
            player._start_slideshow("Show", None)
            player._clear_slideshow()
            player._start_slideshow("Show", None)
        forward_logs = [
            r for r in caplog.records
            if "manifest_schema_version=1.2" in r.getMessage()
        ]
        assert len(forward_logs) == 1, (
            f"forward-version INFO should fire once per digest, got "
            f"{len(forward_logs)} entries"
        )

    def test_unknown_envelope_fields_ignored(self, mpv_player):
        """Forward-compat: a manifest sprouting an unrecognised top-level
        field (from a hypothetical schema_version above the current
        player max) doesn't break the reader.
        """
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        import json
        path = player.assets_dir / "slideshows" / "Show.json"
        path.write_text(json.dumps({
            "name": "Show",
            "manifest_schema_version": "1.2",
            "future_field": "ignored",
            "slides": [{"name": "a.png", "asset_type": "image",
                        "duration_ms": 1000, "play_to_end": False}],
        }))
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        assert player._slideshow is not None
        assert player._slideshow["schema_version"] == "1.2"

    def test_corrupt_schema_version_treated_as_pre_1_0(self, mpv_player, caplog):
        """A garbage value (e.g. ``"abc"``) is parsed as (0,0) which is
        NOT forward of (1,0), so no warning fires.  It just gets stored
        verbatim for diagnostic purposes.
        """
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        self._write_versioned_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 1000, "play_to_end": False},
        ], version="abc")
        with patch.object(svc, "GLib"), caplog.at_level("INFO", logger="agora.player"):
            player._start_slideshow("Show", None)
        assert player._slideshow["schema_version"] == "abc"
        # No forward-version warning for unparseable values.
        assert not any(
            "manifest_schema_version=abc" in r.getMessage()
            for r in caplog.records
        )


class TestSchemaVersionHelpers:
    """Pure-function unit tests for the version comparator."""

    @pytest.fixture
    def svc(self, mpv_player):
        """Reuse the gi-mocked module that mpv_player already imported."""
        _, svc = mpv_player
        return svc

    def test_parse_basic(self, svc):
        assert svc._parse_schema_version("1.0") == (1, 0)
        assert svc._parse_schema_version("1.1") == (1, 1)
        assert svc._parse_schema_version("2.5") == (2, 5)
        assert svc._parse_schema_version("1.0.3") == (1, 0)  # patch ignored

    def test_parse_invalid(self, svc):
        assert svc._parse_schema_version("abc") == (0, 0)
        assert svc._parse_schema_version("") == (0, 0)
        assert svc._parse_schema_version("1") == (0, 0)  # need major.minor
        assert svc._parse_schema_version(None) == (0, 0)  # type: ignore[arg-type]

    def test_forward(self, svc):
        assert svc._is_forward_schema("1.1", "1.0") is True
        assert svc._is_forward_schema("2.0", "1.5") is True
        assert svc._is_forward_schema("1.0", "1.0") is False  # equal not forward
        assert svc._is_forward_schema("0.9", "1.0") is False
        assert svc._is_forward_schema("abc", "1.0") is False


class TestSlideAdvance:
    def test_advance_loops_back_until_count_exceeded(self, mpv_player):
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        (player.assets_dir / "images" / "b.png").touch()
        _write_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 100, "play_to_end": False},
            {"name": "b.png", "asset_type": "image",
             "duration_ms": 100, "play_to_end": False},
        ])
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1
            player._start_slideshow("Show", loop_count=2)
            # 1st slide already shown via _start_slideshow.
            # Walk: a → b → (loop1 incr) a → b → (loop2 incr ≥ target) splash
            for _ in range(4):
                player._on_slide_timeout()
        # After 2nd full loop completes we should be on splash.
        player._show_splash.assert_called_once()
        assert player._slideshow is None

    def test_missing_slide_file_skips_to_next(self, mpv_player):
        player, svc = mpv_player
        # Only second slide exists on disk
        (player.assets_dir / "images" / "b.png").touch()
        _write_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 100, "play_to_end": False},
            {"name": "b.png", "asset_type": "image",
             "duration_ms": 100, "play_to_end": False},
        ])
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1
            player._start_slideshow("Show", loop_count=1)
        # First slide skipped, second slide loaded
        player._loadfile_mpv.assert_called_once()
        loaded_path = player._loadfile_mpv.call_args[0][0]
        assert loaded_path.name == "b.png"

    def test_play_to_end_video_uses_start_mpv_no_loop(self, mpv_player):
        player, svc = mpv_player
        (player.assets_dir / "videos" / "v.mp4").touch()
        _write_manifest(player, "Show", [
            {"name": "v.mp4", "asset_type": "video",
             "duration_ms": 30000, "play_to_end": True},
        ])
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        # play_to_end videos go through _start_mpv with loop=False
        # (mpv exit then advances via _monitor_mpv path).
        player._start_mpv.assert_called_once()
        kwargs = player._start_mpv.call_args.kwargs
        assert kwargs.get("loop") is False
        # No timeout scheduled — exit drives the advance.
        assert player._slideshow["timeout_id"] is None


class TestApplyDesiredRoutes:
    def test_slideshow_asset_type_routes_to_start_slideshow(self, mpv_player):
        player, _ = mpv_player
        desired = DesiredState(
            mode=PlaybackMode.PLAY,
            asset="Show",
            asset_type="slideshow",
        )
        from shared.state import write_state
        write_state(player.desired_path, desired)
        with patch.object(player, "_start_slideshow") as start:
            player.apply_desired()
        start.assert_called_once_with("Show", None)

    def test_same_slideshow_already_running_is_noop(self, mpv_player):
        player, _ = mpv_player
        player._slideshow = {"name": "Show", "slides": [], "index": 0,
                             "loops_completed": 0, "loop_count": None,
                             "timeout_id": None}
        desired = DesiredState(
            mode=PlaybackMode.PLAY,
            asset="Show",
            asset_type="slideshow",
        )
        # Prove the renderer already applied this exact desired; the
        # timestamp/equivalence short-circuit should then no-op without
        # touching _start_slideshow.
        player._applied_desired = desired.model_copy(deep=True)
        from shared.state import write_state
        write_state(player.desired_path, desired)
        with patch.object(player, "_start_slideshow") as start:
            player.apply_desired()
        start.assert_not_called()

    def test_transition_to_splash_clears_slideshow(self, mpv_player):
        player, svc = mpv_player
        player._slideshow = {"name": "Show", "slides": [], "index": 0,
                             "loops_completed": 0, "loop_count": None,
                             "timeout_id": 99}
        desired = DesiredState(mode=PlaybackMode.SPLASH)
        from shared.state import write_state
        write_state(player.desired_path, desired)
        with patch.object(svc, "GLib") as glib:
            player.apply_desired()
        glib.source_remove.assert_called_once_with(99)
        assert player._slideshow is None


class TestPlayToEndIpcDriven:
    """Phase 2: play_to_end advances via mpv IPC event listener.

    When the listener is ready and IPC loadfile reports a
    ``playlist_entry_id``, the slideshow should arm a
    ``pending_play_to_end`` record and rely on ``_on_mpv_event`` to
    advance — not respawn mpv.
    """

    def _arm(self, mpv_player):
        player, svc = mpv_player
        (player.assets_dir / "videos" / "v.mp4").touch()
        _write_manifest(player, "Show", [
            {"name": "v.mp4", "asset_type": "video",
             "duration_ms": 30000, "play_to_end": True},
            {"name": "v.mp4", "asset_type": "video",
             "duration_ms": 5000, "play_to_end": False},
        ])
        # Pretend the IPC event listener is ready and the loadfile
        # captured an entry_id.
        import threading
        player._mpv_event_connected = threading.Event()
        player._mpv_event_connected.set()
        player._mpv_generation = 7

        def fake_loadfile(path, **kw):
            player._mpv_active_entry_id = 42
            return True
        player._loadfile_mpv = MagicMock(side_effect=fake_loadfile)
        return player, svc

    def test_play_to_end_arms_pending_via_ipc(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1234
            player._start_slideshow("Show", None)
        # IPC path used, not respawn.
        player._loadfile_mpv.assert_called_once()
        kwargs = player._loadfile_mpv.call_args.kwargs
        assert kwargs.get("loop") is False
        assert kwargs.get("keep_open") is True
        player._start_mpv.assert_not_called()
        # pending_play_to_end armed with the captured entry_id and current gen.
        pending = player._slideshow["pending_play_to_end"]
        assert pending["entry_id"] == 42
        assert pending["generation"] == 7
        assert pending["watchdog_id"] == 1234

    def test_on_mpv_event_advances_on_matching_eof(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1234
            player._start_slideshow("Show", None)
            # Reset the loadfile mock for the second slide call.
            player._loadfile_mpv.reset_mock()
            player._on_mpv_event({
                "event": "end-file", "reason": "eof",
                "playlist_entry_id": 42, "_generation": 7,
            })
            # Watchdog cancelled, pending cleared, advanced to slide 2.
            glib.source_remove.assert_any_call(1234)
        assert player._slideshow["pending_play_to_end"] is None
        assert player._slideshow["index"] == 2

    def test_on_mpv_event_ignores_mismatched_entry_id(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        before_idx = player._slideshow["index"]
        player._on_mpv_event({
            "event": "end-file", "reason": "eof",
            "playlist_entry_id": 99, "_generation": 7,
        })
        assert player._slideshow["pending_play_to_end"] is not None
        assert player._slideshow["index"] == before_idx

    def test_on_mpv_event_ignores_stale_generation(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        before_idx = player._slideshow["index"]
        player._on_mpv_event({
            "event": "end-file", "reason": "eof",
            "playlist_entry_id": 42, "_generation": 6,
        })
        assert player._slideshow["pending_play_to_end"] is not None
        assert player._slideshow["index"] == before_idx

    def test_on_mpv_event_ignores_stop_reason(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        before_idx = player._slideshow["index"]
        player._on_mpv_event({
            "event": "end-file", "reason": "stop",
            "playlist_entry_id": 42, "_generation": 7,
        })
        assert player._slideshow["pending_play_to_end"] is not None
        assert player._slideshow["index"] == before_idx

    def test_on_mpv_event_advances_on_error_reason(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        player._on_mpv_event({
            "event": "end-file", "reason": "error",
            "playlist_entry_id": 42, "_generation": 7,
        })
        assert player._slideshow["pending_play_to_end"] is None
        assert player._slideshow["index"] == 2

    def test_listener_not_ready_falls_back_to_respawn(self, mpv_player):
        player, svc = mpv_player
        (player.assets_dir / "videos" / "v.mp4").touch()
        _write_manifest(player, "Show", [
            {"name": "v.mp4", "asset_type": "video",
             "duration_ms": 30000, "play_to_end": True},
        ])
        # No _mpv_event_connected attribute → listener not ready.
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        # Legacy path: _start_mpv called, no pending_play_to_end armed.
        player._start_mpv.assert_called_once()
        assert player._slideshow["pending_play_to_end"] is None

    def test_loadfile_failure_falls_back_to_respawn(self, mpv_player):
        player, svc = mpv_player
        (player.assets_dir / "videos" / "v.mp4").touch()
        _write_manifest(player, "Show", [
            {"name": "v.mp4", "asset_type": "video",
             "duration_ms": 30000, "play_to_end": True},
        ])
        import threading
        player._mpv_event_connected = threading.Event()
        player._mpv_event_connected.set()
        player._loadfile_mpv = MagicMock(return_value=False)
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        player._start_mpv.assert_called_once()
        assert player._slideshow["pending_play_to_end"] is None

    def test_loadfile_no_entry_id_falls_back_to_respawn(self, mpv_player):
        player, svc = mpv_player
        (player.assets_dir / "videos" / "v.mp4").touch()
        _write_manifest(player, "Show", [
            {"name": "v.mp4", "asset_type": "video",
             "duration_ms": 30000, "play_to_end": True},
        ])
        import threading
        player._mpv_event_connected = threading.Event()
        player._mpv_event_connected.set()
        # loadfile reports success but never sets entry_id.
        player._mpv_active_entry_id = None
        player._loadfile_mpv = MagicMock(return_value=True)
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        player._start_mpv.assert_called_once()
        assert player._slideshow["pending_play_to_end"] is None

    def test_clear_slideshow_cancels_watchdog(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1234
            player._start_slideshow("Show", None)
        with patch.object(svc, "GLib") as glib:
            player._clear_slideshow()
            # Watchdog id 1234 cancelled (timeout_id is None so only one call).
            glib.source_remove.assert_called_once_with(1234)
        assert player._slideshow is None

    def test_watchdog_advances_when_event_never_arrives(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1234
            player._start_slideshow("Show", None)
        epoch = player._slideshow["epoch"]
        player._on_play_to_end_watchdog(epoch)
        assert player._slideshow["pending_play_to_end"] is None
        assert player._slideshow["index"] == 2

    def test_watchdog_drops_for_stale_epoch(self, mpv_player):
        player, svc = self._arm(mpv_player)
        with patch.object(svc, "GLib"):
            player._start_slideshow("Show", None)
        before_idx = player._slideshow["index"]
        # Fire watchdog from a previous slideshow epoch — should be a no-op.
        player._on_play_to_end_watchdog(player._slideshow["epoch"] - 1)
        assert player._slideshow["pending_play_to_end"] is not None
        assert player._slideshow["index"] == before_idx



class TestScheduledLoopCountIpcDriven:
    """Phase 3: regular schedule finite loop_count via mpv native loop-file=inf.

    Listener counts end-file events; on the Nth match we IPC-load splash.
    No mpv respawn between loops.
    """

    def _arm(self, mpv_player, target_count=3):
        player, svc = mpv_player
        player._scheduled_pending = {
            "entry_id": 17,
            "generation": 5,
            "asset_name": "video.mp4",
            "target_count": target_count,
            "completed_count": 0,
        }
        return player, svc

    def test_eof_increments_count_below_target(self, mpv_player):
        player, _ = self._arm(mpv_player, target_count=3)
        player._on_mpv_event({
            "event": "end-file", "reason": "eof",
            "playlist_entry_id": 17, "_generation": 5,
        })
        assert player._scheduled_pending["completed_count"] == 1
        player._show_splash.assert_not_called()

    def test_eof_at_target_triggers_splash(self, mpv_player):
        player, _ = self._arm(mpv_player, target_count=2)
        player._on_mpv_event({
            "event": "end-file", "reason": "eof",
            "playlist_entry_id": 17, "_generation": 5,
        })
        assert player._scheduled_pending["completed_count"] == 1
        player._show_splash.assert_not_called()
        player._on_mpv_event({
            "event": "end-file", "reason": "eof",
            "playlist_entry_id": 17, "_generation": 5,
        })
        assert player._scheduled_pending is None
        player._show_splash.assert_called_once()

    def test_eof_ignored_for_mismatched_entry_id(self, mpv_player):
        player, _ = self._arm(mpv_player)
        player._on_mpv_event({
            "event": "end-file", "reason": "eof",
            "playlist_entry_id": 99, "_generation": 5,
        })
        assert player._scheduled_pending["completed_count"] == 0
        player._show_splash.assert_not_called()

    def test_eof_ignored_for_stale_generation(self, mpv_player):
        player, _ = self._arm(mpv_player)
        player._on_mpv_event({
            "event": "end-file", "reason": "eof",
            "playlist_entry_id": 17, "_generation": 4,
        })
        assert player._scheduled_pending["completed_count"] == 0
        player._show_splash.assert_not_called()

    def test_stop_reason_ignored(self, mpv_player):
        player, _ = self._arm(mpv_player)
        for reason in ("stop", "quit", "redirect"):
            player._on_mpv_event({
                "event": "end-file", "reason": reason,
                "playlist_entry_id": 17, "_generation": 5,
            })
        assert player._scheduled_pending["completed_count"] == 0
        player._show_splash.assert_not_called()

    def test_error_reason_clears_and_splashes(self, mpv_player):
        player, _ = self._arm(mpv_player)
        player._on_mpv_event({
            "event": "end-file", "reason": "error",
            "playlist_entry_id": 17, "_generation": 5,
        })
        assert player._scheduled_pending is None
        player._show_splash.assert_called_once()

    def test_show_splash_clears_pending_defensively(self, mpv_player):
        player, svc = mpv_player
        player._scheduled_pending = {
            "entry_id": 1, "generation": 1, "asset_name": "a.mp4",
            "target_count": 5, "completed_count": 2,
        }
        player._stop_sway = MagicMock()
        player._find_splash = MagicMock(return_value=None)
        svc.AgoraPlayer._show_splash(player)
        assert player._scheduled_pending is None



class TestMonitorMpvListenerSafetyNet:
    """Phase 4: _monitor_mpv defensively clears stale listener-armed
    pending records when mpv exits, so re-entrant transitions don't fire."""

    def test_rc0_with_scheduled_pending_clears_and_logs(self, mpv_player, caplog):
        player, svc = mpv_player
        # Stub a finished mpv process.
        proc = MagicMock()
        proc.poll.return_value = 0
        proc.stderr.read.return_value = b""
        player._mpv_process = proc
        player._scheduled_pending = {
            "entry_id": 1, "generation": 1, "asset_name": "v.mp4",
            "target_count": 5, "completed_count": 2,
        }
        player.current_desired = svc.DesiredState(
            mode=svc.PlaybackMode.PLAY, asset="v.mp4",
            loop=False, loop_count=None,
        )
        with caplog.at_level("WARNING"):
            with patch.object(svc, "GLib"):
                player._monitor_mpv("v.mp4")
        assert player._scheduled_pending is None
        assert any("listener missed events" in r.message for r in caplog.records)

    def test_rc0_with_slideshow_pending_clears_arm(self, mpv_player):
        player, svc = mpv_player
        proc = MagicMock()
        proc.poll.return_value = 0
        proc.stderr.read.return_value = b""
        player._mpv_process = proc
        player._slideshow = {
            "name": "S", "slides": [], "index": 1, "loop_count": None,
            "loops_completed": 0, "epoch": 1,
            "pending_play_to_end": {
                "entry_id": 1, "generation": 1,
                "slide_index": 1, "slide_name": "x.mp4",
                "watchdog_id": 99,
            },
        }
        player.current_desired = svc.DesiredState(
            mode=svc.PlaybackMode.PLAY, asset="S",
        )
        player._play_next_slide = MagicMock()
        with patch.object(svc, "GLib") as glib:
            player._monitor_mpv("x.mp4")
            glib.source_remove.assert_any_call(99)
        assert player._slideshow["pending_play_to_end"] is None
        player._play_next_slide.assert_called_once()

    def test_error_rc_clears_stale_pendings(self, mpv_player):
        player, svc = mpv_player
        proc = MagicMock()
        proc.poll.return_value = 1
        proc.stderr.read.return_value = b"some error"
        player._mpv_process = proc
        player._scheduled_pending = {
            "entry_id": 1, "generation": 1, "asset_name": "v.mp4",
            "target_count": 3, "completed_count": 0,
        }
        player.current_desired = svc.DesiredState(
            mode=svc.PlaybackMode.PLAY, asset="v.mp4",
        )
        with patch.object(svc, "GLib"):
            player._monitor_mpv("v.mp4")
        assert player._scheduled_pending is None


# === Regression tests for the slideshow short-circuit bug ===

class TestSlideshowShortCircuitRegression:
    """Regression tests for the bug where ending a schedule early left a
    slideshow advancing forever because ``apply_desired`` short-circuited
    on resolved-path equality without verifying renderer state matched
    the new ``desired``.

    Repro on Pi 192.168.1.114 (firmware 1.11.31):
      1. Schedule plays slideshow ``S`` whose first slide is
         ``goodwill_splash.png``.
      2. Operator ends the schedule early; CMS posts desired = STOP /
         then default-asset PLAY of ``goodwill_splash.png``.
      3. Old code: ``_resolve_asset(goodwill_splash.png)`` matched
         ``_current_path`` → fast-path return → slide-advance timer
         survived → slideshow keeps cycling.
    """

    @staticmethod
    def _seed_running_slideshow(player, name="Show", first_slide="a.png",
                                loop_count=None, timeout_id=42, digest="cafebabe"):
        """Pretend slideshow ``name`` is currently mid-flight."""
        (player.assets_dir / "images" / first_slide).touch()
        manifest = _write_manifest(player, name, [
            {"name": first_slide, "asset_type": "image", "duration_ms": 1000},
        ])
        # Use the real digest so _slideshow_manifest_unchanged matches
        # when the test wants the "still running" path.
        import hashlib
        real_digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
        player._slideshow = {
            "name": name,
            "slides": [{"name": first_slide, "asset_type": "image",
                         "duration_ms": 1000}],
            "index": 1,
            "loops_completed": 0,
            "loop_count": loop_count,
            "timeout_id": timeout_id,
        }
        player._slideshow_manifest_digest = real_digest
        # Simulate that slide 1 has been loaded into the renderer.
        player._current_path = player.assets_dir / "images" / first_slide
        player._current_mtime = player._current_path.stat().st_mtime
        player.pipeline = MagicMock()
        # The slideshow itself was applied, so _applied_desired reflects
        # *that*, not a single-asset desired.
        player._applied_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset=name, asset_type="slideshow",
        )
        player.current_desired = player._applied_desired

    def test_smoking_gun_slideshow_to_single_asset_same_path(self, mpv_player):
        """Slideshow active → desired single asset whose resolved path matches
        slide 1. Old code returned without _clear_slideshow(); new code must
        tear down the slideshow."""
        player, svc = mpv_player
        self._seed_running_slideshow(player, name="Show",
                                      first_slide="goodwill_splash.png",
                                      timeout_id=99)
        # New desired: single-asset PLAY of the file that happens to be
        # slide 1. (Real bug: CMS reposted default-asset after STOP.)
        new_desired = DesiredState(
            mode=PlaybackMode.PLAY,
            asset="goodwill_splash.png",
        )
        from shared.state import write_state
        write_state(player.desired_path, new_desired)
        with patch.object(svc, "GLib") as glib:
            with patch.object(player, "_build_pipeline") as build:
                build.return_value = MagicMock()
                player.apply_desired()
        # The pending slide-advance timer MUST be cancelled.
        glib.source_remove.assert_any_call(99)
        # And the slideshow record itself MUST be cleared so a future
        # tick can't restart it.
        assert player._slideshow is None
        assert player._slideshow_manifest_digest is None

    def test_same_slideshow_different_loop_count_restarts(self, mpv_player):
        """Same slideshow name but different loop_count — must rebuild,
        not short-circuit, because user changed playback semantics."""
        player, svc = mpv_player
        self._seed_running_slideshow(player, name="Show", loop_count=None)
        # Reset _applied_desired to reflect the currently-running state
        player._applied_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="Show", asset_type="slideshow",
            loop_count=None,
        )
        new_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="Show", asset_type="slideshow",
            loop_count=3,
        )
        from shared.state import write_state
        write_state(player.desired_path, new_desired)
        with patch.object(player, "_start_slideshow") as start:
            player.apply_desired()
        start.assert_called_once_with("Show", 3)

    def test_same_slideshow_manifest_changed_restarts(self, mpv_player):
        """Same slideshow name + same loop_count, but the manifest file
        was rewritten on disk → must rebuild so new slides take effect."""
        player, svc = mpv_player
        self._seed_running_slideshow(player, name="Show")
        # Rewrite manifest with a different slide list → digest changes.
        _write_manifest(player, "Show", [
            {"name": "b.png", "asset_type": "image", "duration_ms": 2000},
        ])
        (player.assets_dir / "images" / "b.png").touch()
        new_desired = DesiredState(
            mode=PlaybackMode.PLAY, asset="Show", asset_type="slideshow",
        )
        from shared.state import write_state
        write_state(player.desired_path, new_desired)
        with patch.object(player, "_start_slideshow") as start:
            player.apply_desired()
        start.assert_called_once_with("Show", None)

    def test_apply_desired_idempotent_via_predicate(self, mpv_player):
        """Apply same desired twice with different timestamps — the
        timestamp short-circuit can't fire (timestamps differ), so the
        only thing that prevents rebuild is the postcondition predicate.
        Verifies _already_satisfied actually does its job."""
        player, svc = mpv_player
        self._seed_running_slideshow(player, name="Show")
        # Mock _renderer_alive so the predicate can return True without
        # actually inspecting a real subprocess.
        with patch.object(type(player), "_renderer_alive", return_value=True):
            from datetime import datetime, timezone
            d1 = DesiredState(
                mode=PlaybackMode.PLAY, asset="Show", asset_type="slideshow",
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            player._applied_desired = d1.model_copy(deep=True)
            d2 = d1.model_copy(update={
                "timestamp": datetime(2026, 1, 2, tzinfo=timezone.utc),
            })
            from shared.state import write_state
            write_state(player.desired_path, d2)
            with patch.object(player, "_start_slideshow") as start:
                player.apply_desired()
            start.assert_not_called()

    def test_slideshow_to_slideshow_missing_manifest_clears_state(self, mpv_player):
        """Transition from slideshow A to slideshow B; B's manifest is
        missing. _start_slideshow must clear A's state before falling
        back to splash so a stale slide-tick can't restart A."""
        player, svc = mpv_player
        self._seed_running_slideshow(player, name="A", timeout_id=77)
        # No manifest for "B"
        with patch.object(svc, "GLib") as glib:
            player._start_slideshow("B", None)
        glib.source_remove.assert_any_call(77)
        assert player._slideshow is None
        assert player._slideshow_manifest_digest is None
        player._show_splash.assert_called_once()



# ── agora#226 Phase 2: wall-clock-anchored playback ──


class TestLocateSlideAt:
    """Pure-function tests for the ``_locate_slide_at`` helper.

    Each test names the (elapsed_ms, slide-durations) input and the
    expected (target_idx, remaining_ms) output. ``_locate_slide_at`` is
    the math kernel of the anchored playback path — everything else in
    Phase 2 is plumbing around it.
    """

    def _locate(self, *durations):
        # Just builds a slides list with the given durations; field
        # names match the on-disk manifest shape.
        return [{"name": f"s{i}", "duration_ms": d}
                for i, d in enumerate(durations)]

    def test_start_of_cycle(self, mpv_player):
        _, svc = mpv_player
        idx, remaining = svc._locate_slide_at(0, self._locate(1000, 2000, 3000))
        assert idx == 0
        assert remaining == 1000

    def test_mid_first_slide(self, mpv_player):
        _, svc = mpv_player
        idx, remaining = svc._locate_slide_at(400, self._locate(1000, 2000, 3000))
        assert idx == 0
        assert remaining == 600

    def test_exact_boundary_to_second_slide(self, mpv_player):
        """Cycle position == 1000 lands the start of slide 1, not the
        end of slide 0 (half-open intervals).
        """
        _, svc = mpv_player
        idx, remaining = svc._locate_slide_at(1000, self._locate(1000, 2000, 3000))
        assert idx == 1
        assert remaining == 2000

    def test_mid_last_slide(self, mpv_player):
        _, svc = mpv_player
        idx, remaining = svc._locate_slide_at(4500, self._locate(1000, 2000, 3000))
        assert idx == 2
        assert remaining == 1500

    def test_wraps_around_cycle(self, mpv_player):
        """elapsed_ms past one full cycle wraps cleanly."""
        _, svc = mpv_player
        # cycle = 6000; elapsed = 6500 -> pos = 500 -> slide 0, 500ms left.
        idx, remaining = svc._locate_slide_at(6500, self._locate(1000, 2000, 3000))
        assert idx == 0
        assert remaining == 500

    def test_negative_elapsed_wraps(self, mpv_player):
        """A small clock-skew (anchor slightly in the future) wraps to
        the end of the cycle instead of crashing."""
        _, svc = mpv_player
        idx, remaining = svc._locate_slide_at(-200, self._locate(1000, 2000, 3000))
        # -200 % 6000 == 5800 -> 5800-3000(slide0)=2800, 2800-2000(slide1)=800
        # actually: pos=5800; slide0 dur=1000, pos>=1000 -> pos=4800;
        # slide1 dur=2000, pos>=2000 -> pos=2800; slide2 dur=3000, pos<3000
        # -> idx=2, remaining = 3000-2800 = 200.
        assert idx == 2
        assert remaining == 200

    def test_single_slide(self, mpv_player):
        _, svc = mpv_player
        idx, remaining = svc._locate_slide_at(500, self._locate(2000))
        assert idx == 0
        assert remaining == 1500

    def test_empty_slides_raises(self, mpv_player):
        _, svc = mpv_player
        with pytest.raises(ValueError):
            svc._locate_slide_at(0, [])

    def test_all_zero_duration_returns_safe_default(self, mpv_player):
        """Degenerate manifest: every slide has zero duration. Return
        (0, 1) so the caller schedules a 1ms re-eval rather than dividing
        by zero or sleeping forever.
        """
        _, svc = mpv_player
        idx, remaining = svc._locate_slide_at(100, self._locate(0, 0, 0))
        assert idx == 0
        assert remaining == 1


class TestParseIso8601Utc:
    def test_z_suffix(self, mpv_player):
        _, svc = mpv_player
        dt = svc._parse_iso8601_utc("2026-05-23T19:39:45.000Z")
        from datetime import timezone
        assert dt is not None
        assert dt.tzinfo is timezone.utc

    def test_plus_zero_suffix(self, mpv_player):
        _, svc = mpv_player
        dt = svc._parse_iso8601_utc("2026-05-23T19:39:45+00:00")
        assert dt is not None

    def test_invalid_returns_none(self, mpv_player):
        _, svc = mpv_player
        assert svc._parse_iso8601_utc("not-a-date") is None
        assert svc._parse_iso8601_utc("") is None
        assert svc._parse_iso8601_utc(None) is None  # type: ignore[arg-type]


class TestAnchoredPlayback:
    """Mid-cycle reboot → land on the right slide."""

    def _write_anchored(self, player, name, slides, started_at,
                        schema="1.1"):
        import json
        path = player.assets_dir / "slideshows" / f"{name}.json"
        path.write_text(json.dumps({
            "name": name,
            "manifest_schema_version": schema,
            "started_at": started_at,
            "cycle_duration_ms": sum(s["duration_ms"] for s in slides),
            "slides": slides,
        }))
        return path

    def test_anchor_in_past_lands_on_mid_cycle_slide(self, mpv_player):
        """Anchor = 12 min ago, cycle = 5 min, three 100s slides. We
        should land 2 min into cycle 3 → slide index 1.
        """
        player, svc = mpv_player
        from datetime import datetime, timedelta, timezone
        for n in ("a.png", "b.png", "c.png"):
            (player.assets_dir / "images" / n).touch()
        anchor = datetime.now(timezone.utc) - timedelta(minutes=12)
        self._write_anchored(player, "Show", [
            {"name": "a.png", "asset_type": "image", "duration_ms": 100_000,
             "play_to_end": False},
            {"name": "b.png", "asset_type": "image", "duration_ms": 100_000,
             "play_to_end": False},
            {"name": "c.png", "asset_type": "image", "duration_ms": 100_000,
             "play_to_end": False},
        ], started_at=anchor.isoformat().replace("+00:00", "Z"))
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1
            player._start_slideshow("Show", None)
        # 12 min = 720_000 ms; cycle = 300_000 ms; pos = 720_000 % 300_000
        # = 120_000 ms = 2 min in -> slide index 1.
        ss = player._slideshow
        assert ss is not None
        assert ss.get("anchored_current_idx") == 1
        # Resync tick armed at the cap, not the full remaining 180s.
        tick_ms = glib.timeout_add.call_args[0][0]
        assert tick_ms == 5000  # _RESYNC_CAP_MS

    def test_clock_skew_active_falls_back_to_legacy(self, mpv_player, caplog):
        """Anchor far in the future (NTP hasn't synced yet) → use the
        legacy relative-timer chain until the wall clock catches up.
        """
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        from datetime import datetime, timedelta, timezone
        future = datetime.now(timezone.utc) + timedelta(days=365)
        self._write_anchored(player, "Show", [
            {"name": "a.png", "asset_type": "image", "duration_ms": 5000,
             "play_to_end": False},
        ], started_at=future.isoformat().replace("+00:00", "Z"))
        with patch.object(svc, "GLib") as glib, \
                caplog.at_level("INFO", logger="agora.player"):
            glib.timeout_add.return_value = 1
            player._start_slideshow("Show", None)
        ss = player._slideshow
        assert ss is not None
        assert ss.get("clock_skew_active") is True
        # No anchored_current_idx set — went through legacy path.
        assert "anchored_current_idx" not in ss
        assert any(
            "clock-skew guard ACTIVE" in r.getMessage()
            for r in caplog.records
        )

    def test_legacy_manifest_takes_legacy_path(self, mpv_player):
        """schema 1.0 manifest with no anchor → existing index-driven
        chain runs unchanged.
        """
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        _write_manifest(player, "Show", [
            {"name": "a.png", "asset_type": "image",
             "duration_ms": 5000, "play_to_end": False},
        ])
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 7
            player._start_slideshow("Show", None)
        ss = player._slideshow
        assert ss is not None
        # Legacy path advances ss["index"] in lockstep with dispatch.
        assert ss["index"] == 1
        assert "anchored_current_idx" not in ss

    def test_resync_tick_advances_naturally(self, mpv_player):
        """When the resync tick fires and computes the same idx as we
        last dispatched, it just arms the next tick — no snap, no
        re-loadfile.
        """
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        (player.assets_dir / "images" / "b.png").touch()
        from datetime import datetime, timezone
        anchor = datetime.now(timezone.utc)
        self._write_anchored(player, "Show", [
            {"name": "a.png", "asset_type": "image", "duration_ms": 60_000,
             "play_to_end": False},
            {"name": "b.png", "asset_type": "image", "duration_ms": 60_000,
             "play_to_end": False},
        ], started_at=anchor.isoformat().replace("+00:00", "Z"))
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 42
            player._start_slideshow("Show", None)
            ss = player._slideshow
            assert ss["anchored_current_idx"] == 0
            # Fire the resync tick — same epoch, slide 0 still active.
            player._loadfile_mpv.reset_mock()
            result = player._on_anchored_resync_tick(ss["epoch"])
        assert result is False
        # Still on slide 0; no extra loadfile call.
        assert ss["anchored_current_idx"] == 0
        player._loadfile_mpv.assert_not_called()

    def test_chromium_anchored_video_passes_start_offset_ms(self, mpv_player):
        """When the chromium backend is active and we anchored-dispatch
        a video slide partway through its window, ``show_video`` is
        called with ``start_offset_ms = duration_ms - remaining_ms``
        so the browser seeks to the in-progress position instead of
        replaying from t=0."""
        player, svc = mpv_player
        (player.assets_dir / "videos" / "v.mp4").touch()
        # 60s slide, anchored 25s ago -> offset == 25_000 ms.
        from datetime import datetime, timedelta, timezone
        anchor = datetime.now(timezone.utc) - timedelta(seconds=25)
        self._write_anchored(player, "Show", [
            {"name": "v.mp4", "asset_type": "video", "duration_ms": 60_000,
             "play_to_end": False},
        ], started_at=anchor.isoformat().replace("+00:00", "Z"))
        player._use_chromium_backend = True
        player._chromium_player = MagicMock()
        player._chromium_player.asset_url.return_value = "file:///x/v.mp4"
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1
            player._start_slideshow("Show", None)
        player._chromium_player.show_video.assert_called_once()
        kwargs = player._chromium_player.show_video.call_args.kwargs
        # Offset must be within one second of 25_000 ms (test scheduling
        # jitter -- the wall clock advances between _write_anchored
        # and _start_slideshow).
        assert 24_000 <= kwargs["start_offset_ms"] <= 26_000

    def test_chromium_anchored_image_does_not_pass_start_offset(self, mpv_player):
        """Image slides ignore the seek concept -- offset should be 0."""
        player, svc = mpv_player
        (player.assets_dir / "images" / "a.png").touch()
        from datetime import datetime, timedelta, timezone
        anchor = datetime.now(timezone.utc) - timedelta(seconds=25)
        self._write_anchored(player, "Show", [
            {"name": "a.png", "asset_type": "image", "duration_ms": 60_000,
             "play_to_end": False},
        ], started_at=anchor.isoformat().replace("+00:00", "Z"))
        player._use_chromium_backend = True
        player._chromium_player = MagicMock()
        player._chromium_player.asset_url.return_value = "file:///x/a.png"
        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 1
            player._start_slideshow("Show", None)
        player._chromium_player.show_image.assert_called_once()
        # show_video must not have been used for an image slide.
        player._chromium_player.show_video.assert_not_called()

    def test_chromium_play_to_end_overrun_keeps_kiosk_alive(self, mpv_player):
        """Regression: play_to_end overrun on the chromium backend used
        to call ``ChromiumPlayer.stop()`` (full kiosk + shell server
        teardown), which orphaned every subsequent ``show_*`` command
        and left the device frozen on the runaway frame.  The overrun
        path must call ``stop_playback()`` instead -- which clears the
        layer via ``{"cmd":"stop"}`` but leaves the kiosk + WS alive
        for the next slide.
        """
        player, svc = mpv_player
        (player.assets_dir / "videos" / "v.mp4").touch()
        (player.assets_dir / "images" / "n.png").touch()

        from datetime import datetime, timedelta, timezone
        # Anchor far in the past so the resync tick will compute a
        # different target slide and treat the still-armed pending
        # video as an overrun.
        anchor = datetime.now(timezone.utc) - timedelta(seconds=120)
        self._write_anchored(player, "Show", [
            {"name": "v.mp4", "asset_type": "video", "duration_ms": 10_000,
             "play_to_end": True},
            {"name": "n.png", "asset_type": "image", "duration_ms": 10_000,
             "play_to_end": False},
        ], started_at=anchor.isoformat().replace("+00:00", "Z"))

        player._use_chromium_backend = True
        player._chromium_player = MagicMock()
        player._chromium_player.asset_url.side_effect = (
            lambda p: "/assets/" + Path(p).name
        )

        with patch.object(svc, "GLib") as glib:
            glib.timeout_add.return_value = 99
            player._start_slideshow("Show", None)
            ss = player._slideshow
            # Force a clearly-overrun ``armed_at`` so the tick fires the
            # overrun branch deterministically (the slideshow may have
            # landed past slide 0 already; if so, manually re-arm).
            ss["pending_play_to_end_chromium"] = {
                "slide_index": 0,
                "slide_name": "v.mp4",
                "armed_at": datetime.now(timezone.utc) - timedelta(minutes=5),
                "asset_url": "/assets/v.mp4",
            }
            # Reset the call log -- only the overrun-triggered calls
            # should appear.
            player._chromium_player.reset_mock()
            player._on_anchored_resync_tick(ss["epoch"])

        # The fix: stop_playback() (clears layers via WS, kiosk stays
        # alive); stop() (full teardown) must NOT be called.
        assert player._chromium_player.stop_playback.called, (
            "overrun branch must call stop_playback() to clear the "
            "current video without tearing down the kiosk"
        )
        assert not player._chromium_player.stop.called, (
            "overrun branch must NOT call stop() -- that kills the "
            "kiosk + shell server and orphans every subsequent show_*"
        )
