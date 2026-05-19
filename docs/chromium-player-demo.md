# Chromium Player Backend — Demo

Status: **experimental demo, branch `feat/chromium-player-demo`** — not for production, not for merge.

## What this is

A second playback renderer for the agora player. Instead of driving mpv (video)
and GStreamer (images/splash) directly, the player runs an in-process FastAPI
server, launches **one** chromium-in-cage kiosk pointed at it, and drives
playback by sending JSON commands over a WebSocket to a small SPA ("the shell").

Goal: prototype richer still-image transitions (crossfade today, Ken Burns / wipe
/ slide / etc. trivially next) without re-spawning a renderer every slide.

Default behaviour is unchanged. The chromium backend only activates when the
environment variable `AGORA_PLAYER_BACKEND=chromium` is set on the
`agora-player` service.

## Architecture

```
agora-player (Python process)
├── existing supervisor + mpv/gstreamer code paths (unchanged, still the default)
└── ChromiumPlayer (player/chromium_backend.py)
    ├── uvicorn thread serving:
    │     GET  /                 →  player/shell/index.html
    │     GET  /static/*         →  shell CSS/JS
    │     GET  /assets/<path>    →  sandboxed asset reads from /var/lib/agora/...
    │     WS   /ws               →  control channel (one client at a time)
    └── subprocess: cage -- chromium --kiosk http://127.0.0.1:8780/
          └── shell SPA (player/shell/*) — connects to /ws, runs commands
```

One chromium process owns the framebuffer for the lifetime of the player.

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
| Webpage asset         | uses existing cage path             | uses existing cage path       |
| Stream (HLS/RTMP/…)   | falls back to mpv                   | mpv subprocess                |

## Known demo limitations

- **`play_to_end` is best-effort.** The shell's `<video>.ended` event isn't
  wired back through the WebSocket to `_play_next_slide`. Slide advance is
  driven by the slide's `duration_ms` (or 30 s fallback for play_to_end).
- **No `loop_count` IPC parity.** Looped videos run with the HTML `loop`
  attribute; tighter mpv-style loop semantics aren't reproduced.
- **Streams still on mpv.** Easy to add later but skipped for the demo.
- **No backend hot-swap.** Backend is chosen once at startup via env var.
- **Single chromium process owns the display.** If chromium crashes, the
  player as a whole restarts (acceptable for a demo branch).
- **Pi 5 only.** Tested with the user's Pi-5-with-HEVC-chromium setup.
  Pi Zero 2 W and Pi 4 should stay on mpv — they're not part of this demo.

## Enabling on a Pi 5

1. Push the branch's player files onto the device (or build a debian package
   from the branch).
2. Add a systemd drop-in:

   ```bash
   sudo mkdir -p /etc/systemd/system/agora-player.service.d/
   sudo tee /etc/systemd/system/agora-player.service.d/chromium.conf <<'EOF'
   [Service]
   Environment=AGORA_PLAYER_BACKEND=chromium
   EOF
   sudo systemctl daemon-reload
   sudo systemctl restart agora-player
   ```

3. Watch logs: `journalctl -u agora-player -f`. On startup you should see
   `chromium player backend enabled (port=8780)` and then chromium's stderr.
4. Confirm the kiosk window appears with the black shell background.
5. From the CMS, push an image asset and a video asset (HEVC if you want
   to exercise hw decode) at a test device in this group. You should see
   crossfade between them.

To roll back: delete the drop-in and `systemctl restart agora-player`. The
process returns to the mpv code path with no further changes.

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
   everything) and retire the per-asset cage spawns.
4. Decide whether to keep mpv around for streams or move streams into
   chromium too (HLS/DASH would Just Work; RTMP would need media server
   help).
