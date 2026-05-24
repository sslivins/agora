# Chromium Player Backend

Status: **default backend** for image, video, splash, and slideshow playback on production Pis.

## What this is

The primary playback renderer for the agora player. Instead of driving mpv (video)
and GStreamer (images/splash) directly, the player runs an in-process FastAPI
server, launches **one** chromium-in-sway kiosk pointed at it, and drives
playback by sending JSON commands over a WebSocket to a small SPA ("the shell").

Goal: a single persistent renderer that supports richer transitions
(cut/fade/fade_black/dissolve/push/wipe/zoom today, more later) without
re-spawning a process every slide.

The chromium backend is enabled by default.  Set the environment variable
`AGORA_PLAYER_BACKEND=mpv` on the `agora-player` service to fall back to
the legacy mpv / gstreamer code paths (kept around for board bring-up and
as a safety net).

## Architecture

```
agora-player (Python process)
├── legacy mpv/gstreamer code paths (opt-in via AGORA_PLAYER_BACKEND=mpv)
└── ChromiumPlayer (player/chromium_backend.py)
    ├── uvicorn thread serving:
    │     GET  /                 →  player/shell/index.html
    │     GET  /static/*         →  shell CSS/JS
    │     GET  /assets/<path>    →  sandboxed asset reads from /var/lib/agora/...
    │     WS   /ws               →  control channel (one client at a time)
    └── subprocess: systemd-run --scope -- sway -c <conf>
          └── chromium --kiosk http://127.0.0.1:8780/
                └── shell SPA (player/shell/*) — connects to /ws, runs commands
```

One sway+chromium pair owns the framebuffer for the lifetime of the player
(a second sway is spawned by ``AgoraPlayer._start_sway`` when a webpage asset
is desired — the two can't coexist on a single DRM device, so mode-mixing is
a known follow-up).

## Control protocol (player → shell)

JSON sent over `/ws`. Server → client only; the shell does not respond.

```json
{"cmd":"show_image", "url":"/assets/...","transition":"fade","duration_ms":600}
{"cmd":"show_video", "url":"/assets/...","loop":false,"muted":false,"transition":"fade","duration_ms":600}
{"cmd":"show_splash","url":"/assets/...","transition":"cut","duration_ms":0}
{"cmd":"stop"}
```

Coalescing: if commands queue up because the shell is mid-transition or just
reconnected, the buffer keeps only the latest `show_*`. A `stop` after a `show_*`
is kept; a `stop` before a `show_*` is dropped as stale.

Transitions supported: `fade` and `cut` (default). Missing or
unrecognized transition values fall back to `cut` — the CMS is the
source of truth for transition selection. New named transitions are
~5 lines of CSS + a `KNOWN_TRANSITIONS` entry in
`player/shell/player.js`.

## What this demo handles

| asset type            | chromium backend                    | mpv backend (default)         |
|-----------------------|-------------------------------------|-------------------------------|
| Single image          | ✅ `<img>` with CSS crossfade       | imagefreeze pipeline          |
| Single video (file)   | ✅ `<video>` with hw decode         | mpv subprocess                |
| Splash image          | ✅ instant `show_splash`            | imagefreeze pipeline          |
| Splash video          | ✅ looped+muted `show_video`        | mpv subprocess                |
| Slideshow (images/vid)| ✅ driven by `_play_next_slide`     | mpv per-slide                 |
| Webpage asset         | uses existing sway path             | uses existing sway path       |
| Stream (HLS/RTMP/…)   | falls back to mpv                   | mpv subprocess                |

## Known demo limitations

- **Streams still on mpv.** Easy to add later but skipped for the demo.
- **No backend hot-swap.** Backend is chosen once at startup via env var.
- **Single chromium process owns the display.** If chromium crashes, the
  player as a whole restarts (acceptable for a demo branch).
- **Pi 5 only.** Tested with the user's Pi-5-with-HEVC-chromium setup.
  Pi Zero 2 W and Pi 4 should stay on mpv — they're not part of this demo.

### Resolved

- **`play_to_end` is now event-driven.** Slideshow video slides marked
  `play_to_end` advance on the shell's terminal `ended` event, with a
  2× hinted-duration watchdog (60 s floor, 10 min cap) as a safety net.
- **`loop_count` reaches the shell.** Scheduled finite-loop videos pass
  through with `loop_count=N`; the shell counts down in-place (seamless,
  no layer swap) and emits a terminal `ended` that the daemon converts
  to a splash transition.

## Opting back into mpv

The chromium backend is the default on new images.  If you need to fall
back to the legacy mpv / gstreamer path on a specific device (e.g. for
board bring-up or to triage a chromium-only regression), add a systemd
drop-in:

   ```bash
   sudo mkdir -p /etc/systemd/system/agora-player.service.d/
   sudo tee /etc/systemd/system/agora-player.service.d/mpv.conf <<'EOF'
   [Service]
   Environment=AGORA_PLAYER_BACKEND=mpv
   EOF
   sudo systemctl daemon-reload
   sudo systemctl restart agora-player
   ```

To return to the chromium backend, delete the drop-in and `systemctl
restart agora-player`.

## Running the unit tests

```bash
cd <repo>
python -m pytest -p no:timeout tests/test_chromium_backend.py
```

(`-p no:timeout` is required on Windows; pytest-timeout depends on `SIGALRM`
which only exists on Unix.)

The chromium-backend tests do **not** require FastAPI/uvicorn or chromium to
be installed — the relevant imports are lazy and the subprocess spawn is
faked. The tests cover:

- Command JSON shape for every `show_*` / `stop` API
- `_asset_url` path sandboxing (no escape from the assets dir)
- WebSocket attach / detach / publish / coalesce semantics
- Graceful no-op when FastAPI is missing

## Files

- `player/chromium_backend.py` — `ChromiumPlayer` class
- `player/shell/index.html` — kiosk SPA entry point
- `player/shell/player.js` — two-layer crossfade renderer
- `player/shell/player.css` — fullscreen black-bg + transitions
- `player/shell/README.md` — protocol reference
- `tests/test_chromium_backend.py` — unit tests
- `requirements-player.txt` — adds `fastapi`, `uvicorn[standard]` (demo-only)
- `player/service.py` — gated branches in `__init__`, `_show_splash`,
  `apply_desired`, `_play_next_slide`, `_is_showing_splash`,
  `_already_satisfied`, `run()`

## Where to take this next

If the demo holds up on hardware, the obvious follow-ups are:

1. Wire the shell's `ended` / `error` / `loaded` events back to the player
   so `_play_next_slide` and `loop_count` can match mpv behaviour exactly.
2. Add a richer transition catalogue (Ken Burns, slide, dissolve).
3. Move webpage-asset rendering inside the same shell (one chromium for
   everything) and retire the dual-sway split.
4. Decide whether to keep mpv around for streams or move streams into
   chromium too (HLS/DASH would Just Work; RTMP would need media server
   help).
