"""Shared parser for ``/etc/agora/version``.

Both ``os_updater`` (for the floor check on incoming OS update dispatches)
and ``cms_client`` (for reporting ``os_version`` in the register message)
need to read this file. Keeping the parser in one place means a future
regex tightening or key addition only changes one module — no risk of
the two sites drifting and disagreeing about what counts as a valid
version line.

File format::

    # comment lines and blank lines are skipped
    agora_os_version=1.2.3-test
    agora_app_floor=1.11.0

Every non-comment line must be ``key=value``. Whitespace around keys and
values is stripped. Unknown keys are tolerated so this parser doesn't
have to be lockstep with every future field added by agora-os.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

DEFAULT_VERSION_FILE = Path("/etc/agora/version")


def parse_version_file(
    path: Path = DEFAULT_VERSION_FILE,
) -> Dict[str, Optional[str]]:
    """Parse ``/etc/agora/version`` and return a dict of well-known keys.

    Returns a dict with these keys (both may be ``None`` if absent or
    empty in the file):

    * ``agora_os_version`` — the rootfs's baked-in OS version (e.g.
      ``"0.0.16-test"``). Set by the image-build pipeline.
    * ``agora_app_floor`` — the minimum agora-app version this rootfs
      will accept (e.g. ``"1.11.0"``). Set by the image-build pipeline.

    Missing keys are returned as ``None`` rather than raised — callers
    that need a strict floor check (e.g. ``os_updater``) should use
    :func:`read_os_version_strict` instead, which raises if
    ``agora_os_version`` is missing or empty.

    Raises:
        FileNotFoundError: if ``path`` doesn't exist. Callers that want
            to tolerate a missing file (e.g. ``cms_client`` running on a
            dev workstation outside an agora-os device) should catch
            this and fall back to ``None``.
        RuntimeError: if a non-comment, non-blank line is malformed
            (i.e. doesn't contain ``=``).
    """

    result: Dict[str, Optional[str]] = {
        "agora_os_version": None,
        "agora_app_floor": None,
    }

    raw = path.read_text(encoding="utf-8")
    for lineno, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, sep, value = stripped.partition("=")
        if not sep:
            raise RuntimeError(
                f"{path}:{lineno}: malformed line (expected key=value): {line!r}"
            )
        key = key.strip()
        value = value.strip()
        if key in result:
            result[key] = value or None

    return result


def read_os_version_strict(path: Path = DEFAULT_VERSION_FILE) -> str:
    """Read ``agora_os_version`` from ``path``, raising if missing/empty.

    Use this where a missing OS version must be a fatal startup error
    (e.g. ``os_updater`` during the version-floor gate check). For
    best-effort reads (e.g. populating an ``os_version`` field in the
    register payload, where ``None`` is an acceptable fallback), use
    :func:`parse_version_file` directly and handle ``None``.

    Distinguishes "key absent" from "empty value" so error messages are
    actionable when a malformed file is the culprit.

    Raises:
        FileNotFoundError: if ``path`` doesn't exist.
        RuntimeError: if the file is malformed, missing the
            ``agora_os_version`` key, or has an empty value for it.
    """

    raw = path.read_text(encoding="utf-8")
    saw_key = False
    found_value: Optional[str] = None
    for lineno, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, sep, value = stripped.partition("=")
        if not sep:
            raise RuntimeError(
                f"{path}:{lineno}: malformed line (expected key=value): {line!r}"
            )
        if key.strip() == "agora_os_version":
            saw_key = True
            found_value = value.strip()

    if not saw_key:
        raise RuntimeError(
            f"{path} did not contain an 'agora_os_version=...' line"
        )
    if not found_value:
        raise RuntimeError(
            f"{path} had an empty value for 'agora_os_version'"
        )
    return found_value
