# Agora Player Shell

A tiny SPA that runs inside the chromium kiosk and renders image / video
playback commands sent over a WebSocket from the player daemon.

## How it loads

The player daemon (Python) runs a small HTTP+WebSocket server bound to
`127.0.0.1:8780`. It serves these static files at `/`:

- `index.html` — bootstrap, mounts `<div id="stage">` with two layered
  `<div>` children.
- `player.css` — fullscreen black canvas, layered crossfade rules.
- `player.js` — opens `ws://127.0.0.1:8780/ws`, handles commands.

The daemon also serves the asset library at `/assets/`:

- `/assets/images/...`
- `/assets/videos/...`
- `/assets/splash/...`

Chromium is launched (via `sway`, wrapped in a transient systemd scope)
pointing at `http://127.0.0.1:8780/`. It stays up for the life of the
player process.

## Control protocol

JSON over WebSocket. Server → client only matters for control; the shell
sends back a small set of informational events.

### Server → client (commands)

```json
{"cmd":"show_image","url":"/assets/images/foo.jpg",
 "transition":"fade","duration_ms":600}

{"cmd":"show_video","url":"/assets/videos/bar.mp4",
 "loop":true,"muted":false,"transition":"fade","duration_ms":600}

{"cmd":"show_splash","url":"/assets/splash/default.png"}

{"cmd":"stop"}
```

`transition` is `"fade"` or `"cut"` (default). Missing or unrecognized
values fall back to `"cut"` (instant swap) — the CMS is the source of
truth for transition selection; the shell never guesses.
`duration_ms` is the fade animation length when `transition="fade"`;
default 600 ms. Ignored for `"cut"`.

### Client → server (events)

```json
{"event":"ready"}                                        // on WS connect
{"event":"ended","asset":"/assets/videos/bar.mp4"}       // single-play video ended
{"event":"error","asset":"<url>","msg":"<reason>"}       // load failure
```

These are advisory — the daemon uses `ended` for slideshow play-to-end
advancement, and `error` for telemetry/diagnostics.

## Design notes

- Two layered `<div>`s allow crossfading without ever showing black between
  assets. The "old" layer is torn down ~`duration_ms` after the swap to free
  GPU memory (matters for video).
- Auto-reconnects on WS drop with a 1 s backoff. Useful when the player
  daemon restarts but chromium stays up.
- The shell never reads the filesystem directly — only HTTP. That keeps the
  sandbox simple and lets the daemon decide what's served.
