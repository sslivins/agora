"""Provisioning helpers that are independent of the cairo display stack.

Living here (rather than in :mod:`provision.service`) lets tests exercise
the validation logic in CI environments where ``cairo`` and
``gobject-introspection`` are not installed.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger("agora.provision")

# Default location of the bootstrap-v2 pairing secret file.  The
# canonical creator/owner is :mod:`cms_client.bootstrap_boot`; the
# provisioning service only ever *reads* this file.
PERSIST_DIR = Path("/opt/agora/persist")
PAIRING_SECRET_PATH = PERSIST_DIR / "pairing_secret"

# RFC-4648 base32 alphabet (uppercase, no padding).  The pairing secret
# must be exactly 26 chars from this alphabet.  See
# ``shared/bootstrap_identity.py:PAIRING_SECRET_TEXT_LEN``.
_PAIRING_SECRET_RE = re.compile(r"^[A-Z2-7]{26}$")


def read_pairing_secret(path: Path = PAIRING_SECRET_PATH) -> str | None:
    """Read the pairing secret from disk if present and valid.

    Returns ``None`` if the file is missing, unreadable, or does not
    contain a 26-char RFC-4648 base32 string.  Never creates the file —
    creation is owned by ``cms_client.bootstrap_boot``.

    Logs a warning (without echoing the value) on malformed contents so
    the OOBE display can fall back to the non-QR screen.
    """
    try:
        text = path.read_text(encoding="ascii", errors="replace").strip()
    except (FileNotFoundError, OSError):
        return None
    if not _PAIRING_SECRET_RE.fullmatch(text):
        logger.warning(
            "Pairing secret at %s is malformed (len=%d); falling back to "
            "non-QR adoption screen", path, len(text),
        )
        return None
    return text
