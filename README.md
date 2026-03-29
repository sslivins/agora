# Agora

A media playback system for **Raspberry Pi Zero 2 W** that plays video and images on a TV via HDMI, with content managed through a REST API, web UI, and optional central management via [Agora CMS](https://github.com/sslivins/agora-cms).

## Install on Raspberry Pi Zero 2 W

### Prerequisites

1. Flash **Raspberry Pi OS 64-bit Lite** onto an SD card using [Raspberry Pi Imager](https://www.raspberrypi.com/software/)
2. In Imager settings, enable SSH and configure Wi-Fi
3. Boot the Pi and ensure it has network connectivity

### Install

```bash
curl -fsSL https://raw.githubusercontent.com/sslivins/agora/main/scripts/setup-pi.sh | sudo bash
```

This adds the Agora apt repository, installs the package, and starts all services. When complete it prints the web UI URL and default credentials.

To upgrade later:

```bash
sudo apt update && sudo apt upgrade agora
```

## Architecture

Three services communicate through JSON state files on disk and a WebSocket connection to the CMS:

### API Service (port 8000)

FastAPI application running via systemd. Provides:

- **REST API** (`/api/v1/`) — asset upload/delete/list, playback control, status, CMS configuration
- **Web UI** (`/`) — Jinja2 dashboard for managing assets, playback, and settings from a browser
- **Auth** — API key header (`X-API-Key`) for programmatic access, signed session cookies for the web UI

### Player Service

GStreamer-based media player running natively via systemd to access hardware:

- Watches `desired.json` via inotify (2s polling fallback)
- Builds GStreamer pipelines for video (`v4l2h264dec` → `kmssink` + HDMI audio via ALSA) and images (`decodebin` → `imagefreeze` → `kmssink`)
- Supports looping, automatic splash screen fallback on EOS/error
- Reports actual state to `current.json`

### CMS Client Service

WebSocket client that maintains a persistent connection to [Agora CMS](https://github.com/sslivins/agora-cms):

- Registers device by CPU serial number with auth token
- Receives schedule windows and caches them locally
- Evaluates schedules locally every 15 seconds
- Pre-fetches upcoming assets with budget-aware LRU eviction
- Accepts live commands: play, stop, config updates, reboot
- Exponential backoff on connection errors (2s → 60s cap)

### State Machine

```
API writes desired.json  →  Player reads & acts  →  Player writes current.json  →  API reads for status
CMS Client receives schedule → writes desired.json → Player acts
```

## Web UI Pages

| Page | Path | Description |
|------|------|-------------|
| Dashboard | `/` | Current playback state, cached schedule display |
| Assets | `/assets` | Upload, list, delete media files, set splash screen |
| Playback | `/playback` | Manual play/stop/splash controls |
| Settings | `/settings` | Device info, storage usage, CMS connection config |
| Login | `/login` | Web authentication |

## API Endpoints

All `/api/v1/` endpoints require authentication (`X-API-Key` header or session cookie) unless noted.

### Status

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/health` | Health check (no auth) — device name, version, uptime |
| `GET` | `/api/v1/status` | Current/desired state, asset count, schedule hash |

### Playback

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/play` | Play an asset `{"asset": "file.mp4", "loop": true}` |
| `POST` | `/api/v1/stop` | Stop playback, show splash |
| `POST` | `/api/v1/splash` | Show splash screen |

### Assets

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/assets` | List all assets |
| `POST` | `/api/v1/assets/upload` | Upload a media file (max 500 MB) |
| `DELETE` | `/api/v1/assets/{name}` | Delete an asset |
| `POST` | `/api/v1/assets/{name}/set-splash` | Set asset as active splash screen |
| `DELETE` | `/api/v1/assets/splash` | Clear splash override, revert to default |

### CMS Configuration

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/cms/config` | CMS connection status and config |
| `POST` | `/api/v1/cms/config` | Set CMS server address and port |

## Directory Structure

```
/opt/agora/
├── assets/
│   ├── videos/        # Uploaded .mp4 files
│   ├── images/        # Uploaded .jpg/.jpeg/.png files
│   └── splash/        # Splash screen assets
├── state/
│   ├── desired.json   # What the player should do (written by API / CMS client)
│   ├── current.json   # What the player is doing (written by player)
│   ├── cms_config.json    # CMS connection settings
│   ├── cms_auth_token     # Device auth token from CMS
│   ├── schedule.json      # Cached schedule from CMS
│   └── assets.json        # Asset manifest (checksums, sizes, LRU)
├── logs/
└── src/               # Source code
```

## Configuration

Loaded from `/boot/agora-config.json`, overlaid by `AGORA_` environment variables:

```json
{
    "api_key": "your-secure-api-key",
    "web_username": "admin",
    "web_password": "agora",
    "secret_key": "your-signing-secret",
    "device_name": "breakroom-01",
    "cms_url": "ws://192.168.1.100:8080/ws/device"
}
```

Keys are auto-generated on first boot if not set. See `config/agora-config.example.json` for the full template.

## Supported Formats

- **Video:** `.mp4` (H.264, hardware-decoded via V4L2)
- **Images:** `.jpg`, `.jpeg`, `.png`

## CMS Protocol (WebSocket)

Protocol version: **1**

### Device → CMS

| Type | Description |
|------|-------------|
| `register` | Device ID, auth token, firmware version, storage capacity |
| `status` | Heartbeat: playback state, disk usage, uptime (every 30s) |
| `fetch_request` | Request an asset from CMS |
| `asset_ack` | Confirm asset downloaded |
| `asset_deleted` | Confirm asset removed |

### CMS → Device

| Type | Description |
|------|-------------|
| `auth_assigned` | Initial auth token for new device |
| `sync` | Full schedule window, timezone, default asset |
| `play` | Immediate playback command |
| `stop` | Stop playback |
| `fetch_asset` | Download URL + checksum + size |
| `delete_asset` | Remove local asset |
| `config` | Update splash, password, API key, device name |
| `reboot` | Reboot device |

## Development

### Requirements

- **API:** FastAPI, uvicorn, Jinja2, itsdangerous, pydantic-settings (`requirements-api.txt`)
- **Player:** GStreamer 1.0 with GI bindings, inotify-simple (`requirements-player.txt`)
- **CMS Client:** websockets, aiohttp, pydantic (`requirements-cms-client.txt`)
- **Tests:** pytest, pytest-asyncio, httpx (`requirements-test.txt`)

### Running Tests

```bash
pytest tests/ --tb=short -q
```

### Releasing

The **Create Release** workflow (Actions → Create Release → Run workflow) reads the version from `api/__init__.py`, creates a git tag, builds the `.deb` package, publishes a GitHub Release, and updates the apt repository.

Bump the version in `api/__init__.py` before running.

## Related

- **[Agora CMS](https://github.com/sslivins/agora-cms)** — Central management server for scheduling and fleet control
