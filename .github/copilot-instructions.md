# Agora — Copilot Instructions

## Project Overview

Agora is a media playback system for **Raspberry Pi Zero 2 W**. It plays video/images on a TV via HDMI, with content uploaded and controlled through a REST API and web UI.

## Architecture

Two processes, communicating via JSON state files on disk (`desired.json` and `current.json` in `/opt/agora/state/`):

1. **API service** — FastAPI app running in Docker on port 8000. Handles asset management (upload, list, delete), playback control (play/stop/splash), status reporting, and a Jinja2 web UI. Auth via `X-API-Key` header or signed session cookies.

2. **Player service** — Runs natively via systemd (not containerized) to access hardware. Uses GStreamer for media playback: `v4l2h264dec` + `kmssink` for H.264 video, `imagefreeze` + `kmssink` for images, ALSA for HDMI audio. Watches `desired.json` via inotify, writes `current.json` to report actual state.

## Key Design Decisions

- **File-based IPC**: No direct communication between API and player. API writes `desired.json`, player reads it and writes `current.json`. Atomic file writes via temp file + `os.replace()`.
- **Player runs natively**: Must access KMS/DRM, V4L2 hardware decoder, and ALSA — cannot run in Docker.
- **API runs in Docker**: Isolation for the network-facing service.
- **Config from `/boot/agora-config.json`**: Easy to configure on SD card before first boot, overlaid by `AGORA_` env vars.

## Source Layout

- `api/` — FastAPI application (main.py, config.py, auth.py, ui.py, routers/, static/, templates/)
- `player/` — GStreamer player service (main.py, service.py)
- `shared/` — Pydantic models and state file I/O shared between API and player
- `config/` — Example configuration
- `systemd/` — systemd unit file for the player service

## Tech Stack

- **Python 3.11**, **FastAPI**, **Pydantic v2**, **uvicorn**
- **GStreamer 1.0** via PyGObject (gi.repository)
- **inotify-simple** for file watching (with polling fallback)
- **itsdangerous** for signed session cookies
- **Docker** for the API service only

## Conventions

- Pydantic models for all data structures (shared/models.py)
- Atomic file writes everywhere (shared/state.py)
- Filename validation via regex whitelist in asset uploads
- 500 MB max upload size
- Assets organized into `videos/`, `images/`, `splash/` subdirectories under `/opt/agora/assets/`
- Supported formats: `.mp4` (video), `.jpg`/`.jpeg`/`.png` (images)
- API version lives in `api/__init__.py` (`__version__`)
- **Whenever API endpoints are added, changed, or removed, update `docs/openapi.yaml` to match.**

## Hardware Target

Raspberry Pi Zero 2 W — ARM Cortex-A53, limited RAM/CPU. Keep resource usage minimal. GStreamer pipelines use hardware H.264 decoding (`v4l2h264dec`) and KMS display sink (`kmssink`).
