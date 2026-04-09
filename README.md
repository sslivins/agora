# Agora

[![Tests](https://github.com/sslivins/agora/actions/workflows/tests.yml/badge.svg)](https://github.com/sslivins/agora/actions/workflows/tests.yml)
[![Release](https://img.shields.io/github/v/release/sslivins/agora)](https://github.com/sslivins/agora/releases/latest)

**Turn any TV into a managed digital signage display with a $15 Raspberry Pi Zero 2 W.**

Agora is a lightweight media playback system that runs on a Raspberry Pi, playing video and images on any HDMI-connected screen. Upload content through a local web UI or REST API, or connect to [Agora CMS](https://github.com/sslivins/agora-cms) for centralized scheduling and fleet management across dozens of displays.

**No cloud services, no subscriptions, no vendor lock-in.** Everything runs on your own hardware.

### Highlights

- **Zero-config setup** — Flash an SD card, plug in, and connect via your phone's Wi-Fi
- **Hardware-accelerated playback** — H.264 video decoded in hardware via V4L2, smooth 1080p on a $15 board
- **Web UI + REST API** — Upload assets, control playback, and monitor status from any browser or script
- **Fleet-ready** — Optional CMS connection for centralized scheduling, remote control, and over-the-air updates
- **Captive portal provisioning** — On first boot, the device creates a Wi-Fi hotspot with a guided setup wizard
- **Self-healing** — Automatic reconnection, schedule caching, and graceful fallback to splash screens

## Install on Raspberry Pi Zero 2 W

### Option 1: Pre-Built Image (Recommended)

Download the latest release image from [GitHub Releases](https://github.com/sslivins/agora/releases) — the file is named `agora-v{version}-pi-zero2w.img.xz`.

Flash it to an SD card using [Raspberry Pi Imager](https://www.raspberrypi.com/software/):

1. Open Raspberry Pi Imager
2. Choose **Use custom** and select the downloaded `.img.xz` file
3. Select your SD card and write

On first boot the device starts a Wi-Fi access point (**Agora-XXXX**) with a captive-portal web UI for configuring Wi-Fi, device name, and CMS connection. No SSH or manual config needed.

### Option 2: Install on Existing Raspberry Pi OS

Start with **Raspberry Pi OS 64-bit Lite**. In Imager settings, enable SSH and configure Wi-Fi, then:

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
- Accepts live commands: play, stop, config updates, reboot, SSH toggle
- Exponential backoff on connection errors (2s → 60s cap)

### State Machine

```
API writes desired.json  →  Player reads & acts  →  Player writes current.json  →  API reads for status
CMS Client receives schedule → writes desired.json → Player acts
```

### Provisioning (First Boot)

On a fresh image with no Wi-Fi configured, the device enters **AP mode** (access point named `Agora-XXXX`). A captive-portal web UI lets the user:

1. Scan and select a Wi-Fi network
2. Set a device name
3. Optionally configure CMS connection

After saving, the device reboots into client mode and connects to the chosen network. If Wi-Fi is lost later, the device cycles between retry and temporary AP mode until connectivity is restored.

## Web UI Pages

| Page | Path | Description |
|------|------|-------------|
| Dashboard | `/` | Current playback state, cached schedule display |
| Assets | `/assets` | Upload, list, delete media files, set splash screen |
| Playback | `/playback` | Manual play/stop/splash controls |
| Settings | `/settings` | Device info, storage usage, CMS connection config |
| Login | `/login` | Web authentication |

## API

The full REST API is documented in [docs/openapi.yaml](docs/openapi.yaml). You can explore it interactively using the [Swagger Editor](https://editor.swagger.io/?url=https://raw.githubusercontent.com/sslivins/agora/main/docs/openapi.yaml).

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
| `status` | Heartbeat: playback state, disk usage, uptime, CPU temp (every 30s) |
| `fetch_request` | Request an asset from CMS |
| `fetch_failed` | Download failed with reason and budget info |
| `asset_ack` | Confirm asset downloaded with checksum |
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
| `config` | Update splash, password, API key, device name, SSH access |
| `reboot` | Reboot device |
| `upgrade` | Trigger firmware upgrade |

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

The **Create Release** workflow (Actions → Create Release → Run workflow) reads the version from `api/__init__.py`, creates a git tag, builds the `.deb` package, publishes a GitHub Release with the `.deb` and the Pi image (`agora-v{version}-pi-zero2w.img.xz`), and updates the apt repository.

Bump the version in `api/__init__.py` before running.

## Related

- **[Agora CMS](https://github.com/sslivins/agora-cms)** — Central management server for scheduling and fleet control
