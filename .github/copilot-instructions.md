# Agora — Copilot Instructions

## Project Overview

Agora is a media playback system for **Raspberry Pi Zero 2 W**. It plays video/images on a TV via HDMI, with content uploaded and controlled through a REST API and web UI.

## Architecture

Two processes, communicating via JSON state files on disk (`desired.json` and `current.json` in `/opt/agora/state/`):

1. **API service** — FastAPI app running via systemd on port 8000. Handles asset management (upload, list, delete), playback control (play/stop/splash), status reporting, and a Jinja2 web UI. Auth via `X-API-Key` header or signed session cookies.

2. **Player service** — Runs natively via systemd to access hardware. Uses GStreamer for media playback: `v4l2h264dec` + `kmssink` for H.264 video, `imagefreeze` + `kmssink` for images, ALSA for HDMI audio. Watches `desired.json` via inotify, writes `current.json` to report actual state.

## Key Design Decisions

- **File-based IPC**: No direct communication between API and player. API writes `desired.json`, player reads it and writes `current.json`. Atomic file writes via temp file + `os.replace()`.
- **Player runs natively**: Must access KMS/DRM, V4L2 hardware decoder, and ALSA.
- **API runs natively**: Both services run as systemd units for simplicity.
- **Config from `/boot/agora-config.json`**: Easy to configure on SD card before first boot, overlaid by `AGORA_` env vars.

## Source Layout

- `api/` — FastAPI application (main.py, config.py, auth.py, ui.py, routers/, static/, templates/)
- `player/` — GStreamer player service (main.py, service.py)
- `cms_client/` — WebSocket client for CMS connection (service.py, main.py)
- `shared/` — Pydantic models and state file I/O shared between API and player
- `config/` — Example configuration
- `systemd/` — systemd unit files for all services

## Tech Stack

- **Python 3.11**, **FastAPI**, **Pydantic v2**, **uvicorn**
- **GStreamer 1.0** via PyGObject (gi.repository)
- **inotify-simple** for file watching (with polling fallback)
- **itsdangerous** for signed session cookies
- **systemd** for all services (API, player, CMS client)

## Conventions

- Pydantic models for all data structures (shared/models.py)
- Atomic file writes everywhere (shared/state.py)
- Filename validation via regex whitelist in asset uploads
- 500 MB max upload size
- Assets organized into `videos/`, `images/`, `splash/` subdirectories under `/opt/agora/assets/`
- Supported formats: `.mp4` (video), `.jpg`/`.jpeg`/`.png` (images)
- API version lives in `api/__init__.py` (`__version__`)
- **Whenever API endpoints are added, changed, or removed, update `docs/openapi.yaml` to match.**

## Bug Fixing — Test-Driven

- **Before fixing any bug, write a failing test that reproduces it.** Confirm the test fails, then implement the fix, then confirm the test passes.
- Tests live in `tests/` and use pytest + pytest-asyncio + httpx.

## Git Workflow

- **`main` is sacred** — never commit directly to `main`.
- All changes must be made on a feature branch and merged via pull request.
- Branch naming: `feat/<short-description>`, `fix/<short-description>`, `chore/<short-description>`, `perf/<short-description>`, `refactor/<short-description>`, `docs/<short-description>`, `test/<short-description>`, `ci/<short-description>`.
- **Never merge a PR** unless the user explicitly asks you to. Creating PRs is fine; merging requires explicit approval.
- Bump the version in `api/__init__.py` and `docs/openapi.yaml` when shipping user-facing changes.
- **After creating a PR, always check CI status** using `gh pr checks <number>` or `gh run list`. Monitor until all checks pass. If any fail, inspect the logs with `gh run view <run-id> --log-failed`, fix issues, push fixes, and re-check until green.

## Commit Messages — Conventional Commits

All commit messages **must** use [Conventional Commits](https://www.conventionalcommits.org/) format. The release workflow auto-generates changelogs from these prefixes.

**Format:** `<type>(<optional scope>): <description>`

| Prefix | When to use | Example |
|---|---|---|
| `feat:` | New feature or capability | `feat: add device group scheduling` |
| `fix:` | Bug fix | `fix: prevent player crash on missing asset` |
| `perf:` | Performance improvement | `perf: reduce GStreamer pipeline startup time` |
| `refactor:` | Code restructuring (no behavior change) | `refactor: extract asset validation helper` |
| `test:` | Adding or updating tests only | `test: add OOBE provisioning flow tests` |
| `docs:` | Documentation only | `docs: update OpenAPI spec for new endpoints` |
| `ci:` | CI/CD workflow changes | `ci: add changelog generation to release workflow` |
| `chore:` | Maintenance, deps, tooling | `chore: bump FastAPI to 0.115` |

- Use the **imperative mood** in descriptions: "add" not "added", "fix" not "fixes".
- Optional scope in parentheses: `fix(player): handle missing codec gracefully`.
- Keep the first line under 72 characters.
- Add a blank line + body for complex changes.

## Hardware Target

Raspberry Pi Zero 2 W — ARM Cortex-A53, limited RAM/CPU. Keep resource usage minimal. GStreamer pipelines use hardware H.264 decoding (`v4l2h264dec`) and KMS display sink (`kmssink`).
