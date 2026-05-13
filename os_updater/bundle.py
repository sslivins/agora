"""Bundle format + integrity stubs — implemented by sibling todo p2-bundle-format.

Phase 2 ships this module as a placeholder so the import graph is stable.
The real implementation will:

* Parse ``meta.json`` (version, min_from_version, sha256_manifest, created_at, builder)
* Verify every unpacked file's sha256 against the manifest
* Surface a typed ``BundleIntegrityError`` for the service to map to
  ``failed:bundle_invalid``

See plan.md §"Phase 2 — Deliverables" and ``docs/bundle-format.md``
(to be written by the same sibling).
"""

from __future__ import annotations


class BundleError(Exception):
    """Base class for bundle-related failures."""


class BundleIntegrityError(BundleError):
    """sha256 manifest mismatch or missing file."""


def parse_bundle_meta(*args, **kwargs):  # noqa: D401
    raise NotImplementedError("see sibling todo p2-bundle-format")


def verify_bundle_manifest(*args, **kwargs):  # noqa: D401
    raise NotImplementedError("see sibling todo p2-bundle-format")
