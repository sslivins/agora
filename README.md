# Agora

A media playback system for Raspberry Pi Zero 2 W that plays video and images on a TV, with content managed via a REST API and web UI.

## Architecture

Two processes communicate through JSON state files on disk:

### API Service (systemd)

A FastAPI application running natively via systemd on port 8000. Provides:

- **REST API** (`/api/v1/`) — asset upload/delete/list, playback control (play, stop, splash), status and health endpoints
- **Web UI** (`/`) — Jinja2-based dashboard for managing assets and playback from a browser
- **Auth** — API key header (`X-API-Key`) for programmatic access, signed session cookies for the web UI

### Player Service (systemd)

A GStreamer-based media player that runs natively via systemd to access hardware:

- Watches `desired.json` via inotify (2s polling fallback)
- Builds GStreamer pipelines for video (`v4l2h264dec` → `kmssink` + HDMI audio via ALSA) and images (`decodebin` → `imagefreeze` → `kmssink`)
- Supports looping, automatic splash screen fallback on EOS/error
- Reports its actual state to `current.json`

### State Machine

```
API writes desired.json  →  Player reads & acts  →  Player writes current.json  →  API reads for status
```

## Directory Structure

```
/opt/agora/
├── assets/
│   ├── videos/        # Uploaded .mp4 files
│   ├── images/        # Uploaded .jpg/.jpeg/.png files
│   └── splash/        # Splash screen assets (shown on idle/startup)
├── state/
│   ├── desired.json   # What the player should be doing (written by API)
│   └── current.json   # What the player is actually doing (written by player)
├── logs/
└── src/               # Source code (player runs from here)
```

## Configuration

Config is loaded from `/boot/agora-config.json`, overlaid by `AGORA_` environment variables.

```json
{
    "api_key": "your-secure-api-key",
    "web_username": "admin",
    "web_password": "your-secure-password",
    "secret_key": "your-signing-secret",
    "device_name": "breakroom-01"
}
```

See `config/agora-config.example.json` for the template.

## Playback Modes

| Mode | Description |
|---|---|
| `play` | Play a specific asset (video or image), optionally looping |
| `stop` | Stop all playback |
| `splash` | Show the splash screen (auto-loops if video) |

## Supported Formats

- **Video:** `.mp4` (H.264, played via hardware decoder)
- **Images:** `.jpg`, `.jpeg`, `.png`

## API Endpoints

All `/api/v1/` endpoints require authentication (`X-API-Key` header or session cookie).

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/health` | Health check (no auth required) |
| `GET` | `/api/v1/status` | Current and desired state, asset count |
| `POST` | `/api/v1/assets/upload` | Upload a media file (multipart form) |
| `GET` | `/api/v1/assets` | List all assets |
| `DELETE` | `/api/v1/assets/{name}` | Delete an asset |
| `POST` | `/api/v1/play` | Play an asset `{"asset": "file.mp4", "loop": true}` |
| `POST` | `/api/v1/stop` | Stop playback |
| `POST` | `/api/v1/splash` | Show splash screen |

## Deployment

### API (systemd)

```bash
sudo cp systemd/agora-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now agora-api
```

### Player (systemd)

```bash
sudo cp systemd/agora-player.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now agora-player
```

The player runs natively as a systemd service to access GStreamer, KMS, and ALSA hardware directly.

## Requirements

- **API:** Python 3.11, FastAPI, uvicorn, itsdangerous, pydantic-settings (see `requirements-api.txt`)
- **Player:** Python 3, GStreamer 1.0 with GI bindings, inotify-simple (see `requirements-player.txt`)
- **Hardware:** Raspberry Pi Zero 2 W, HDMI-connected display
