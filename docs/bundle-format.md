# Agora OS Update Bundle Format

This document defines the on-the-wire format of a signed Agora OS update bundle as consumed by `agora-os-updater` and produced by the `sslivins/agora-os` build pipeline. It is the contract between the build side (CI) and the device side (`agora-os-updater`); any change to it is a breaking change to one or the other.

See also: [bundle-migration.md](./bundle-migration.md) for the `/data` forward-migration runner.

## At a glance

A release is **two files** delivered side-by-side as GitHub Release assets:

| File | Content | Mime type |
|---|---|---|
| `agora-os-<version>.tar.zst` | zstd-compressed tarball of the bundle | `application/octet-stream` |
| `agora-os-<version>.tar.zst.minisig` | detached [minisign](https://jedisct1.github.io/minisign/) signature over the `.tar.zst` bytes | `text/plain` |

The CMS dispatch carries the URLs of both files plus `target_version`, `min_from_version`, `release_id`, and the `force_now` / `force_downgrade` flags. The device downloads both, verifies the signature against the `.tar.zst` bytes, unpacks, verifies the manifest, and only then stages the contents into the inactive slot.

## File naming

```
agora-os-<version>.tar.zst
agora-os-<version>.tar.zst.minisig
```

- `<version>` is the same SemVer string declared inside `meta.json` (`major.minor.patch`, optionally `-prerelease`). It matches `^\d+\.\d+\.\d+(?:-[A-Za-z0-9.]+)?$` — the same regex the dispatch validator enforces.
- The minisign sig file ALWAYS has the `.minisig` extension (minisign's default), not `.sig`. The dispatch payload's `signature_url` SHOULD point at the `.minisig`, but the device is willing to accept a `.sig` extension as well so an operator can rename it without breaking anything.

## Tarball layout

After zstd decompression, the tar archive contains exactly three top-level entries:

```
boot/        directory — contents to write to the inactive boot partition
root/        directory — contents to write to the inactive rootfs partition
meta.json    file      — bundle metadata + sha256 manifest
```

**No other top-level entries are permitted.** A bundle that contains extra top-level files or directories is treated as corrupted and rejected with `failed:bundle_invalid`.

### `boot/`

A tree of files that will be rsync'd into the inactive boot partition (`/boot/firmware-<inactive>/` on the device). Includes per-slot `cmdline.txt`, `config.txt`, `autoboot.txt`, kernel + DTB, etc. The tree MUST already reflect the target slot — i.e. `cmdline.txt`'s `root=PARTLABEL=root-A` vs `root-B` is baked in at build time by `image-build/assemble.sh` (see D51 in the project plan); the device-side updater does not rewrite cmdline.txt.

The build pipeline emits two parallel `boot/` trees, one keyed `boot-A` and one keyed `boot-B`. **A single bundle ships exactly one** — whichever matches the inactive slot at dispatch time. The dispatcher in the CMS chooses the right asset based on per-device state. (If we later need a single bundle to cover both slots, that's a future bundle-format-v2 problem.)

> **v1 simplification.** For v1 we ship the **same** `boot/` content regardless of target slot because `cmdline.txt` is the only file that differs and `agora-slot-mgr` is responsible for templating it post-stage. This is enabled by D51 (PARTLABEL=root-A/B in cmdline.txt being templated server-side per slot). If that ever changes (e.g. signed initramfs with per-slot embedded paths), this section gets revisited.

### `root/`

A tree of files that will be rsync'd into the inactive root partition (`/mnt/inactive-root/` on the device). Same structure as a normal Raspberry Pi OS rootfs. The tree is laid out **without** per-device identity files (no `/etc/machine-id`, no `/etc/ssh/ssh_host_*`); the on-device `agora-firstboot.service` regenerates those on first boot of the new slot. This matches what the image build does for fresh flashes (F11).

The tree DOES include:

- `/etc/agora/update-pubkey.pem` and `/etc/agora/update-pubkey-recovery.pem` (D54) — usually unchanged across releases, but a release that rotates either key will ship the new version here.
- `/etc/agora/update-pubkeys.d/*.pem` — additional pubkeys for the **directory-lookup verifier** (see "Multi-key verification" below). v1 ships this directory empty; v2+ uses it for in-flight key rotation.
- `/etc/agora/migrations/NNN_description.sh` — `/data` forward-migration scripts (see [bundle-migration.md](./bundle-migration.md)).
- `/usr/share/agora/release.json` — a tiny pinned copy of `meta.json` so a running rootfs always knows what version it is, independent of the bundle staging area.

### `meta.json`

A single JSON file at the bundle root. Schema:

```jsonc
{
  // SemVer string. MUST match the filename. Validated against the same
  // regex as the dispatch payload's target_version.
  "version": "1.4.0",

  // SemVer string. The minimum version of the running rootfs that may
  // accept this bundle. Floor only — the device runs:
  //
  //   current >= min_from_version  → allowed
  //   current <  min_from_version  → reject with failed:version_floor
  //
  // UNLESS the dispatch payload sets "force_downgrade": true, in which
  // case this check is skipped entirely (Decision #24).
  "min_from_version": "1.2.0",

  // Schema version of /data that this rootfs supports. Used by the
  // forward-migration runner (see bundle-migration.md). The runner
  // applies every migration NNN in /etc/agora/migrations/ where
  // NNN > current /data/SCHEMA_VERSION and NNN <= this value.
  // Post-promote only; gated by the Phase 1 migration-allowed sentinel.
  "schema_version": 2,

  // Map of relative-path-inside-tarball → lowercase hex sha256 of that
  // file's content, computed by the build pipeline. Every regular
  // file under boot/ and root/ MUST appear here. Directories, symlinks,
  // and device nodes are NOT listed. The on-device verifier:
  //
  //   1. unpacks the tarball into the staging directory,
  //   2. for each entry in sha256_manifest, opens the on-disk file and
  //      streams sha256 over it,
  //   3. compares to the manifest value,
  //   4. on any mismatch OR any extra regular file not in the manifest,
  //      rejects with failed:bundle_invalid and deletes the staging dir.
  //
  // Keys are forward-slash paths relative to tarball root, no leading
  // slash, no "./" prefix. Example: "boot/cmdline.txt".
  "sha256_manifest": {
    "boot/cmdline.txt":     "a3f1c8...",
    "boot/config.txt":      "9b2e44...",
    "boot/kernel8.img":     "1d7c30...",
    "root/usr/bin/agora":   "f0a812...",
    "root/etc/agora/...":   "..."
  },

  // ISO 8601 timestamp at which the build pipeline assembled the
  // tarball. Informational only — not verified by the device, but
  // logged in lifecycle events so the operator can correlate to a
  // CI run.
  "created_at": "2026-05-12T18:33:01Z",

  // String identifying the build that produced this bundle. Format
  // is "<repo>@<commit-sha>+<workflow-run-id>", e.g.
  //   "sslivins/agora-os@074bfce+25772660193"
  // Also informational; not verified.
  "builder": "sslivins/agora-os@074bfce+25772660193"
}
```

`meta.json` is itself listed in the sha256_manifest? **No.** `meta.json` is excluded from the manifest because it would be self-referential. Its integrity is guaranteed by the minisign signature over the entire tarball, which is verified BEFORE the manifest is even read.

Forward-compatibility: unknown top-level keys in `meta.json` are silently ignored by the v1 device-side parser. This lets future bundles carry hints (e.g. `delta_from_version`, `prepare_command`) that older devices safely no-op on.

## Compression

The bundle is a **zstd-compressed tarball** (`.tar.zst`, magic bytes `28 b5 2f fd`).

Why zstd, not xz:

- Decompression is ~4× faster than xz at comparable ratios on a Pi 5. The compressed Pi 5 rootfs is ~1.1 GB; xz costs ~90s of CPU on-device, zstd costs ~20s. (D55 / F19.)
- Same algorithm as the image distribution (`.img.zst`), so the device only needs one decompressor implementation.

Zstd compression level is `-19 --long=27` in CI. The window-log of 27 (`--long=27`, 128 MB) means the decompressor must be invoked with a matching `--long=27` (or higher); the device-side `zstd` binary in the bundled rootfs is built with `ZSTD_WINDOWLOG_MAX_64=27` to permit this.

The tar inside the zstd is plain POSIX `ustar`. Long filenames use `pax` headers; the device-side `tar` (busybox-tar is NOT used; we ship GNU tar) handles both.

## Signature scheme

Minisign with an Ed25519 keypair (D4). Each bundle ships a single detached `.minisig` file produced by:

```
minisign -S -s /path/to/agora-os-signing.key -m agora-os-<version>.tar.zst
```

This signs the raw bytes of `agora-os-<version>.tar.zst` AFTER zstd compression — i.e. the bytes the device will receive over HTTP, **not** the post-decompression tar bytes. This means:

- The device can verify the signature without decompressing anything (cheap rejection of tampered downloads).
- A re-compression of the same tar content with a different zstd level invalidates the signature. That's a feature: it forces all signed bundles to share the exact compression settings CI uses.

**Trusted comment.** Minisign embeds a "trusted comment" line into the `.minisig` that is itself signed. The build pipeline writes:

```
trusted comment: agora-os <version> built <created_at> by <builder>
```

The on-device verifier does not currently parse this string (the JSON inside the tarball is the source of truth), but it is logged at INFO level for forensics.

### Multi-key verification (directory lookup, v1)

The device-side verify code searches `/etc/agora/update-pubkeys.d/*.pem` first, then falls back to the two baked pubkeys:

1. `/etc/agora/update-pubkeys.d/*.pem` (sorted, accepted in any order)
2. `/etc/agora/update-pubkey.pem` (primary, baked in by Phase 0 image build)
3. `/etc/agora/update-pubkey-recovery.pem` (recovery, baked in by Phase 0 image build)

Verification SUCCEEDS the moment any one key validates the signature. Verification FAILS only if all of them reject.

This directory-lookup pattern is the foundation for in-flight key rotation in v2. In v1 the rotation procedure is:

1. Ship release N adding the new pubkey to `/etc/agora/update-pubkeys.d/new.pem`, still signed with the old primary key.
2. Wait for the fleet to promote N.
3. Ship release N+1 signed with the new key (now all devices trust both).
4. Ship release N+2 removing the old pubkey.

The signing key currently used by CI lives as a GH Actions secret `MINISIGN_SECRET`, scoped to the signing job per D53.

## Size envelope

- Compressed tarball: target 800 MB – 1.2 GB. Hard limit 1.5 GB (anything bigger is rejected by the dispatcher upstream because it won't fit in the `/data/.update/staging/` budget alongside the unpacked tree).
- Unpacked tree on disk: ~2.2 GB (boot/ + root/ + meta.json), of which ~2.1 GB is `root/`.
- Per Decision #20, the device requires **~2 GB free on `/data`** to accept a bundle. The Phase 1 precheck enforces this; the updater re-checks it just before download to avoid racing a long-running log spike.

## End-to-end verification flow (device side)

This is the contract `agora-os-updater` honors. Each step's failure produces a specific lifecycle event:

1. **Receive dispatch** over WPS WebSocket. Parse the payload; on any schema problem emit `failed:bundle_invalid` (we treat malformed dispatch as a bundle-level problem because the only practical effect is the same).
2. **Pre-admission floor check.** If `current_version < min_from_version` and `force_downgrade != true`, emit `failed:version_floor` and stay in `IDLE`.
3. **Download** the `.tar.zst` to `/data/.update/staging/<release_id>/bundle.tar.zst` with resumable HTTP (`Range:` requests on retry). On unrecoverable HTTP error, emit `failed:download` and clean the staging dir.
4. **Download** the `.minisig` (small, single shot). Same failure path as step 3.
5. **Verify signature** over the on-disk `.tar.zst` bytes using the multi-key directory lookup above. Failure: emit `failed:signature_invalid`, delete staging dir immediately. Do NOT decompress on signature failure — the tarball might be malicious.
6. **Decompress + extract** into `/data/.update/staging/<release_id>/unpacked/`. Stream-decompress with the bundled zstd; pipe through GNU tar with `--no-same-owner --no-same-permissions=root` (the device-side rsync stage will fix ownership). Failure: `failed:bundle_invalid`.
7. **Read `meta.json`.** Reject if `version` mismatches the dispatch's `target_version` (defense in depth — the CMS should never dispatch a mismatched bundle, but we don't trust the network). Failure: `failed:bundle_invalid`.
8. **Verify manifest.** Walk `boot/` and `root/`; for every regular file confirm it appears in `sha256_manifest` AND the on-disk content sha256 matches. Reject on missing entries, extra files, or hash mismatch. Failure: `failed:bundle_invalid`, delete staging.
9. **Stage to inactive slot.** Rsync `boot/` → `/boot/firmware-<inactive>/`, `root/` → `/mnt/inactive-root/`. (Implemented in p2-stage-and-tryboot.)
10. **Hand off to slot-mgr.** Invoke `agora-slot-mgr trigger-tryboot` to flip the next-boot pointer. This is the last device-side step before reboot; after reboot, Phase 1's slot-confirm logic takes over.
11. **`/data` migrations run POST-PROMOTE ONLY** (see [bundle-migration.md](./bundle-migration.md)). They are NOT part of the staging step — they fire after slot-confirm flips `agora-firstboot.service`'s migration-allowed sentinel.

## Staging directory lifecycle

`/data/.update/staging/` is owned by the updater and structured as:

```
/data/.update/staging/
  <release_id>/
    bundle.tar.zst        # the downloaded asset
    bundle.tar.zst.minisig
    unpacked/             # populated by step 6 above
      boot/
      root/
      meta.json
```

Each dispatch's `release_id` namespaces its own subdirectory. Cleanup happens at:

- **Step 5 failure** (signature) — delete `<release_id>/` immediately. Belt-and-braces: untrusted bytes never sit on disk for more than seconds.
- **Step 6–8 failure** (decompress / manifest) — delete `<release_id>/` immediately.
- **Step 10 success** — keep `<release_id>/` until reboot. After reboot, slot-confirm's success path deletes it; slot-confirm's rollback path leaves it for forensics (the rollback diagnostics in Phase 1 already collect it).
- **`agora-os-updater.service` start** — sweep `/data/.update/staging/*/` and delete any directory whose `mtime` is more than 24h old OR whose `meta.json` is missing/unparseable (TTL + sanity). This handles the case where the device rebooted unexpectedly mid-stage. (Decision #20.)

## Future-compat notes

- **Delta updates.** A future bundle format could replace `root/` with a casync `castr/` + index file. The minisign + `meta.json` envelope stays identical; only steps 6–9 of the verify flow change. The `builder` and `version` fields are forward-compatible.
- **A separate manifest file.** If `meta.json` grows large enough to be annoying to embed (e.g. >100k sha256 entries), splitting into `meta.json` + `manifest.json` is a one-version flag day. The signature scheme doesn't care; the device-side parser does, hence flagging here.
- **Cosign / sigstore.** Out of scope for v1. Minisign was chosen for being self-contained and not requiring a transparency log we'd then have to operate.
