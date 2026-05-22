"""Parsing + validation of the ``os_update_dispatch`` WPS payload.

The CMS dispatches a release to a device by pushing an ``os_update_dispatch``
control message over the existing WPS connection. The payload is the same
shape that's stored in the ``scheduled_dispatches`` row (plan.md §"Phase 3 —
DB schema additions"), minus the row-bookkeeping fields:

    {
      "type": "os_update_dispatch",
      "release_id": "rel_2026_05_07_v1.1.0",
      "target_version": "1.1.0",
      "min_from_version": "1.0.0",
      "bundle_url": "https://github.com/.../bundle.zst",
      "signature_url": "https://github.com/.../bundle.zst.minisig",
      "force_now": false,
      "force_downgrade": false
    }

Phase 3 may add fields (``not_before`` for staggered rings, etc.) — the
parser uses ``pydantic`` with ``extra="ignore"`` so a forward-compatible
field doesn't break older daemons.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DispatchPayloadError(ValueError):
    """Raised when an ``os_update_dispatch`` payload fails validation.

    The daemon treats this as a routine failure path — log + emit
    ``failed:invalid_payload`` over WPS — not a crash.
    """


#: Regex matching ``major.minor.patch`` semver with optional ``-prerelease``.
#: The prerelease segment allows hyphens inside it (e.g. ``0.0.40-test-k612``)
#: so multi-token prereleases produced by our agora-os release flow pass.
#: Intentionally simple — we control both ends of this wire so we don't need
#: full semver-2.0 compliance.
_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$")

#: Regex matching the allowed shape of an opaque ``release_id``: alphanumeric
#: plus ``_-.``, length 1..128. Mirrors what the CMS will generate.
_RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


class DispatchPayload(BaseModel):
    """Validated form of a CMS-side ``os_update_dispatch`` message."""

    model_config = ConfigDict(extra="ignore")

    release_id: str
    target_version: str
    min_from_version: str
    bundle_url: str
    signature_url: str
    force_now: bool = False
    force_downgrade: bool = False

    #: Optional Phase 3 fields that older daemons can simply ignore.
    not_before: Optional[str] = Field(default=None)

    @field_validator("release_id")
    @classmethod
    def _check_release_id(cls, value: str) -> str:
        if not _RELEASE_ID_RE.match(value):
            raise ValueError(
                "release_id must match [A-Za-z0-9._-]{1,128}"
            )
        return value

    @field_validator("target_version", "min_from_version")
    @classmethod
    def _check_version(cls, value: str) -> str:
        if not _VERSION_RE.match(value):
            raise ValueError(
                "version must be major.minor.patch (with optional -prerelease)"
            )
        return value

    @field_validator("bundle_url", "signature_url")
    @classmethod
    def _check_url(cls, value: str) -> str:
        if not (value.startswith("https://") or value.startswith("http://")):
            raise ValueError("url must use http(s) scheme")
        return value


def parse_dispatch_payload(msg: Any) -> DispatchPayload:
    """Parse an inbound WPS message into a :class:`DispatchPayload`.

    The caller has already routed by ``msg["type"] == "os_update_dispatch"``.
    Wraps the pydantic ``ValidationError`` in :class:`DispatchPayloadError`
    so the caller doesn't need to import pydantic just to catch payload
    failures.
    """

    if not isinstance(msg, Mapping):
        raise DispatchPayloadError(
            f"dispatch payload must be a JSON object, got {type(msg).__name__}"
        )

    try:
        return DispatchPayload.model_validate(dict(msg))
    except Exception as exc:
        raise DispatchPayloadError(str(exc)) from exc
