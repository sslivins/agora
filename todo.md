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
- [x] Define CMS data model (devices, groups, schedules, assets)
- [x] Define CMS ↔ device WebSocket protocol (message types, auth handshake)
- [x] Device-side: registration client + WebSocket sync client
- [x] Device-side: asset pre-fetch + cleanup logic
- [x] CMS: device management API + UI (approve, group, configure)
- [x] CMS: schedule management API + UI (create, recurring rules, priorities)
- [x] CMS: asset library (upload once, assign to devices/groups)
- [x] CMS: push asset to device (transfer over WebSocket or HTTP download?)
- [x] CMS: dashboard (device status, what's playing, connectivity)
- [x] Handle CMS unavailability gracefully (keep playing current content, poll for reconnect)

## Scheduled Playback (Device-Local — Superseded by CMS)
- [x] ~~Design schedule data model~~ — handled by CMS
- [x] ~~Add schedule storage~~ — PostgreSQL in CMS
- [x] ~~Scheduler service~~ — CMS scheduler pushes to device
- [x] ~~API endpoints: CRUD for schedules~~ — CMS REST API
- [x] ~~Web UI: schedule management page~~ — CMS web UI
- [x] ~~Handle overlapping schedules / priority rules~~ — CMS priority system
- [x] ~~Return to splash when scheduled playback ends~~ — CMS sync handles this

## Features
- [ ] Asset preview thumbnails in web UI
- [ ] Playlist support (ordered sequence of assets)
- [ ] Volume control via API
- [x] Multi-device management (control multiple Pis from one UI) — via CMS

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
- [x] CI/CD pipeline (GitHub Actions: lint, test)
- [ ] Automated deployment script (scp + systemctl restart)
- [ ] Log viewer in web UI
- [ ] Backup/restore configuration

## Bugs / Polish
- [x] Reduce pipeline startup time (~5s black gap during transitions and at boot)
  - Root cause: kmssink's `kms_open()` probed 13 wrong DRM drivers (~320ms each) before reaching `vc4`
  - Fix: added `driver-name=vc4` to all kmssink pipelines (PR #46)
  - Result: pipeline startup dropped from ~5s to <1s
- [ ] Wi-Fi AP fallback not triggering — when a provisioned device can't connect to Wi-Fi on boot, it starts up normally instead of switching to AP mode.
  - **Root cause (likely):** `is_wifi_connected()` in `provision/network.py` only checks if NetworkManager has an "active" 802-11-wireless connection (`nmcli connection show --active`). NM can show a saved Wi-Fi profile as "activated" (associated with AP) before it actually has an IP or before it discovers the network is unreachable. The 60s `_wait_for_wifi` loop polls every 2s and returns True on this false positive, so the service exits cleanly and never enters AP mode.
  - **Fix options:** (a) After `is_wifi_connected()` returns True, verify actual IP connectivity (e.g. `nmcli -t -f IP4.ADDRESS device show wlan0` or ping the gateway). (b) Use `nmcli -t -f GENERAL.STATE device show wlan0` and check for `100 (connected)` instead of just checking the connection profile.
  - **Secondary issue:** `agora-provision.service` uses `Type=simple` — the `Before=agora-api.service` ordering doesn't actually block other services from starting until provision finishes. Consider `Type=oneshot` + `RemainAfterExit=yes` for the provisioned boot path, or split into a separate oneshot unit that gates the other services.
- [ ] Handle missing HDMI gracefully (no display connected)
- [ ] Watchdog: auto-restart player if pipeline hangs
- [ ] HDMI CEC integration — CEC confirmed working on Pi Zero 2 W via `cec-ctl` (no sudo needed, `video` group has `/dev/cec0` access). Commands: `cec-ctl --to 0 --standby` (TV off), `cec-ctl --to 0 --image-view-on` (TV on + switch input). Features: standby TV after splash timeout (configurable, e.g. 30min), wake TV on play command, expose on/off via API/CMS
- [ ] Splash screen pixel shifting — subtle periodic shift to reduce burn-in on static splash image
