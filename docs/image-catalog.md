# Pi Image Catalog

Every published Agora release includes a `catalog.json` manifest
describing the bootable Pi images attached to that release. Downstream
consumers (e.g. the Agora CMS imager service) use this manifest to
discover available images and verify their integrity before importing
them into their own storage.

## Where to find it

Two URL patterns are stable and supported:

| Use case                     | URL                                                                                |
|---                           |---                                                                                 |
| **Latest stable** (rolling)  | `https://github.com/sslivins/agora/releases/latest/download/catalog.json`         |
| **Pinned version** (immutable) | `https://github.com/sslivins/agora/releases/download/<tag>/catalog.json`        |

`releases/latest/...` auto-resolves to the most recent non-prerelease
on GitHub.

> **Publish ordering:** When a release is first cut, it is created as a
> *pre-release*. The Pi image build is long (~1–2 h) and runs after the
> tag is created. Only when **all three image variants build, verify,
> and upload successfully** is the release promoted to a normal release
> (and thus to `releases/latest/`). This means:
>
> - During the build window, `releases/latest/download/catalog.json`
>   continues to resolve to the **previous stable release** — never to
>   a half-published one.
> - If the image build fails (any variant), the release stays a
>   pre-release. `releases/latest/...` is never broken; consumers see
>   no transition until the next successful release.
> - Per-version URLs (`releases/download/<tag>/...`) only become
>   resolvable after the catalog publish step succeeds.

## Schema

```json
{
  "schemaVersion": 1,
  "generatedAt": "2026-04-29T14:23:01Z",
  "version": "v1.11.32",
  "variants": [
    {
      "variant": "pi5",
      "version": "v1.11.32",
      "filename": "agora-v1.11.32-pi5.img.xz",
      "url": "https://github.com/sslivins/agora/releases/download/v1.11.32/agora-v1.11.32-pi5.img.xz",
      "sha256": "abc123...def",
      "compressedBytes": 612345678,
      "uncompressedBytes": 8589934592
    },
    { "variant": "pi4", ... },
    { "variant": "pi-zero2w", ... }
  ]
}
```

### Field semantics

| Field               | Type    | Notes                                                           |
|---                  |---      |---                                                              |
| `schemaVersion`     | int     | Bumped only on breaking schema changes                          |
| `generatedAt`       | string  | RFC 3339 UTC timestamp                                          |
| `version`           | string  | Release tag (with `v` prefix)                                   |
| `variants[].variant`| string  | One of `pi5`, `pi4`, `pi-zero2w`                                |
| `variants[].url`    | string  | Direct download URL for the `.img.xz`                           |
| `variants[].sha256` | string  | Hex SHA256 of the compressed `.img.xz` bytes                    |
| `variants[].compressedBytes` | int | Size of the `.img.xz` on disk                              |
| `variants[].uncompressedBytes` | int | Size of the underlying `.img` after `xz -d` (from xz metadata) |

### Sidecar files

Each release also publishes:

- `<filename>.sha256` — single-line `<sha>  <basename>` per image
- `SHA256SUMS` — concatenated for the whole release; `sha256sum -c` compatible

## Consumer expectations

Consumers MUST:
1. Fetch the catalog over HTTPS
2. Verify `schemaVersion == 1` (or warn and treat unknown variants as opaque)
3. Pin downloaded images by SHA256 — do not trust URLs alone
4. Treat `releases/latest/download/catalog.json` as the rolling stable
   pointer (changes on every published release)

## Immutability guarantees

Per-tag asset URLs (`releases/download/<tag>/...`) are treated as
**immutable**: once the catalog publish job has succeeded for a tag,
re-running the workflow for that tag will fail rather than silently
overwrite the existing assets. Re-publishing a release deliberately
requires manually deleting the existing assets first
(`gh release delete-asset <tag> <name>`) — and bumping the version is
strongly preferred.

## Image properties

The published images are **tenant-agnostic**. They contain the
firmware, services, and dependencies for the target board, but no
CMS URL, no WiFi credentials, and no device identity. Downstream
provisioning systems are expected to inject `/boot/firmware/agora-fleet.env`
into the FAT boot partition before flashing — typically via `mcopy`
on the offset reported by `parted -s -j`.
