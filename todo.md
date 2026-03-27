# Agora — TODO

## Scheduled Playback
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

## Infrastructure
- [ ] CI/CD pipeline (GitHub Actions: lint, test)
- [ ] Automated deployment script (scp + systemctl restart)
- [ ] Log viewer in web UI
- [ ] Backup/restore configuration

## Bugs / Polish
- [ ] Reduce pipeline startup time (~4s gap during transitions)
- [ ] Handle missing HDMI gracefully (no display connected)
- [ ] Watchdog: auto-restart player if pipeline hangs
