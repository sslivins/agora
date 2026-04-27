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
        p._cage_process = None
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
        m = player._read_slideshow_manifest("Show")
        assert m is not None
        assert m["name"] == "Show"
        assert len(m["slides"]) == 1

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
        player._stop_cage = MagicMock()
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
