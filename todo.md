# Agora — TODO

## CMS (Agora Central Management System)

### Architecture
- Device-initiated WebSocket connection to CMS (solves NAT/firewall issues)
- CMS is the single source of truth — devices are thin clients
- Schedule is held **in memory only** on the device (no flash writes)
- On reboot, device re-registers with CMS and pulls fresh state
- If CMS is unreachable, device polls until it reconnects — CMS downtime is expected to be rare

### Registration Flow
- Device has a unique ID (e.g. Pi serial from `/proc/cpuinfo` or UUID generated on first boot)
- On boot, device connects to CMS WebSocket endpoint, sends ID + auth token
- CMS creates device as "pending" if new — admin approves in CMS UI
- Once approved, device receives its config (splash screen, group/location, schedule)

### Communication Model (Hybrid Pull + WebSocket Push)
- **On connect**: device pulls full schedule + current asset assignment from CMS
- **Live updates**: CMS pushes change notifications over the open WebSocket (schedule change, play now, new asset)
- **Reconnect**: on any disconnect (reboot, network blip), device re-registers and pulls fresh state
- No local schedule persistence — if the device can't reach the CMS, it has no schedule

### Asset Management (Flash-Aware)
- Pi has limited flash storage — design for **at most 2 videos** on device at any time (current + next)
- CMS must be aware of device storage capacity
- Schedule can define playback far into the future (e.g. Christmas video set in summer) but assets are only pushed to the device when needed
- CMS pre-fetches the next scheduled asset before its start time
- When an asset is no longer needed, CMS instructs device to delete it
- Must support devices with varying storage — guarantee at least 1 video fits

### Schedule Model
- CMS stores the full schedule per device (or per device group)
- Supports: one-time (date + time range), recurring (e.g. every Mon/Wed/Fri 11am–2pm), default/fallback content
- Device receives only the relevant portion — what to play now + what's next
- Priority system for overlapping schedules (e.g. "emergency" content overrides regular schedule)

### CMS Stack (Proposed)
- FastAPI (consistent with device-side stack)
- PostgreSQL for schedule/device/asset storage
- WebSocket endpoint for device connections
- Web UI for managing devices, schedules, asset library
- ~30 concurrent device WebSocket connections (trivial load)

### TODO
- [ ] Define CMS data model (devices, groups, schedules, assets)
- [ ] Define CMS ↔ device WebSocket protocol (message types, auth handshake)
- [ ] Device-side: registration client + WebSocket sync client
- [ ] Device-side: asset pre-fetch + cleanup logic
- [ ] CMS: device management API + UI (approve, group, configure)
- [ ] CMS: schedule management API + UI (create, recurring rules, priorities)
- [ ] CMS: asset library (upload once, assign to devices/groups)
- [ ] CMS: push asset to device (transfer over WebSocket or HTTP download?)
- [ ] CMS: dashboard (device status, what's playing, connectivity)
- [ ] Handle CMS unavailability gracefully (keep playing current content, poll for reconnect)

## Scheduled Playback (Device-Local — Superseded by CMS)
- [ ] Design schedule data model (cron-like or time-range based?)
- [ ] Add schedule storage (JSON file or SQLite?)
- [ ] Scheduler service that writes `desired.json` at trigger times
- [ ] API endpoints: CRUD for schedules
- [ ] Web UI: schedule management page
- [ ] Handle overlapping schedules / priority rules
- [ ] Return to splash when scheduled playback ends

## Features
- [ ] Asset preview thumbnails in web UI
- [ ] Playlist support (ordered sequence of assets)
- [ ] Volume control via API
- [ ] Multi-device management (control multiple Pis from one UI)

## Captive Portal Provisioning
- [x] Provisioning FastAPI service (provision/)
- [x] NetworkManager Wi-Fi helpers (scan, connect, AP mode)
- [x] Captive portal setup page (branded UI)
- [x] dnsmasq DNS redirect for captive portal detection
- [x] Boot flow: unprovisioned → AP mode; provisioned → Wi-Fi retry → AP fallback
- [x] Factory reset endpoint (wipes assets, Wi-Fi, config, reboots)
- [x] Factory reset button on settings page
- [x] systemd service (agora-provision.service, runs before API/player/CMS client)
- [ ] mDNS via avahi for agora-XXXX.local discovery
- [x] Auto-discover CMS via mDNS (agora-cms.local) during provisioning
- [ ] Install script updates (dnsmasq, NetworkManager packages)

## Infrastructure
- [ ] CI/CD pipeline (GitHub Actions: lint, test)
- [ ] Automated deployment script (scp + systemctl restart)
- [ ] Log viewer in web UI
- [ ] Backup/restore configuration

## Bugs / Polish
- [x] Reduce pipeline startup time (~5s black gap during transitions and at boot)
  - Root cause: kmssink's `kms_open()` probed 13 wrong DRM drivers (~320ms each) before reaching `vc4`
  - Fix: added `driver-name=vc4` to all kmssink pipelines (PR #46)
  - Result: pipeline startup dropped from ~5s to <1s
- [ ] Handle missing HDMI gracefully (no display connected)
- [ ] Watchdog: auto-restart player if pipeline hangs
