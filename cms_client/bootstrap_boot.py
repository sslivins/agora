"""Device-side bootstrap orchestration (stage B.3 of issue #420).

Wraps :mod:`cms_client.bootstrap_client` (pure HTTP primitives) and
:mod:`shared.bootstrap_identity` (crypto + secret-file primitives) into
the two entry points the service lifecycle actually needs:

* :func:`ensure_wps_credentials` — used at connect time.  Returns a
  valid ``(wps_url, wps_jwt, expires_at)`` triple, choosing between
  three branches:

    1. cached state exists and JWT not yet expired → return as-is;
    2. cached state exists but JWT is expired → signed
       ``/connect-token`` to mint a fresh one;
    3. no cached state → first-boot path: register, poll until adopted,
       decrypt the outbox, then signed ``/connect-token``.

* :func:`refresh_wps_jwt` — used by the background renewal task.  Always
  goes through the signed ``/connect-token`` path.  Assumes the device
  is already adopted; if it isn't, the call will raise
  :class:`ConnectTokenRejectedError` and the caller decides policy.

The state file (``<persist_dir>/bootstrap_state.json``, mode 0600) is
the adopted marker.  It records the CMS base URL the device was adopted
against so we can invalidate on a CMS URL change.

Invariants this module enforces:

* Seeds and JWTs never appear in log messages at INFO or above.
* State writes are atomic (temp-file + ``os.replace``) and fsynced.
* Cached JWT is treated as expired ``JWT_EARLY_REFRESH_SEC`` seconds
  before its notarized ``expires_at`` — this prevents the "connect with
  JWT that expires 2 seconds later" race.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from shared.bootstrap_identity import (
    DeviceIdentity,
    decrypt_adopt_payload,
    load_or_create_device_identity,
    load_or_create_pairing_secret,
    pairing_secret_hash_hex,
)

from cms_client.bootstrap_client import (
    BootstrapServerError,
    BootstrapTransportError,
    ConnectTokenRejectedError,
    ConnectTokenResult,
    PendingNotFoundError,
    PubkeyMismatchError,
    RateLimitedError,
    fetch_connect_token,
    get_bootstrap_status_once,
    register_once,
)

logger = logging.getLogger("agora.cms_client.bootstrap_boot")


STATE_SCHEMA_VERSION = 1
# Treat the cached JWT as expired this many seconds before its notarized
# expiry.  Guards against opening a WPS connection with a JWT that dies
# seconds later.
JWT_EARLY_REFRESH_SEC = 60
# Default poll cadence for GET /bootstrap-status during first-boot.  The
# operator may take seconds to minutes to click "Adopt".  Start fast,
# back off to reduce CMS load for abandoned pending devices.
FIRST_BOOT_POLL_SHORT_SEC = 5.0
FIRST_BOOT_POLL_LONG_SEC = 30.0
# Switch to the long cadence after this many short-cadence polls.
FIRST_BOOT_POLL_SHORT_COUNT = 24  # ~2 minutes at 5s cadence


class BootstrapConfigError(RuntimeError):
    """Caller asked for bootstrap v2 but missing config (fleet_id/secret)."""


class BootstrapCancelledError(Exception):
    """First-boot poll loop was cancelled externally."""


@dataclass(frozen=True)
class BootstrapCredentials:
    """Result of :func:`ensure_wps_credentials` / :func:`refresh_wps_jwt`."""

    wps_url: str
    wps_jwt: str
    expires_at: str  # RFC3339 UTC string from the CMS
    device_id: str  # the device_id used at adopt time (Pi serial for agora)


# ---------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------


def _parse_expires_at(expires_at: str) -> Optional[datetime]:
    """Parse an RFC3339 UTC timestamp; return None on garbage."""
    if not expires_at:
        return None
    # Tolerate the "...Z" suffix that CMS emits.
    s = expires_at.replace("Z", "+00:00") if expires_at.endswith("Z") else expires_at
    try:
        dt = datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def load_state(path: Path) -> Optional[dict]:
    """Load cached bootstrap state, or return None if missing/unreadable."""
    try:
        raw = path.read_text()
    except (FileNotFoundError, OSError):
        return None
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("bootstrap_state.json corrupt — ignoring")
        return None
    if not isinstance(data, dict) or data.get("schema_version") != STATE_SCHEMA_VERSION:
        logger.warning(
            "bootstrap_state.json has unsupported schema_version=%r — ignoring",
            data.get("schema_version") if isinstance(data, dict) else None,
        )
        return None
    return data


def save_state(path: Path, state: dict) -> None:
    """Atomically write state, fsync, chmod 0600."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, separators=(",", ":"), sort_keys=True).encode("utf-8")
    fd, tmp_name = tempfile.mkstemp(
        prefix=".bootstrap_state.", suffix=".tmp", dir=str(path.parent)
    )
    tmp_path = Path(tmp_name)
    try:
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
        # fsync the parent dir so the rename is durable
        try:
            dir_fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


def clear_state(path: Path) -> None:
    """Best-effort delete; never raises."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _is_state_fresh(state: dict, cms_api_base: str, now_unix: float) -> bool:
    """Is the cached state valid for ``cms_api_base`` and not near expiry?"""
    if state.get("cms_api_base") != cms_api_base:
        logger.info(
            "Cached bootstrap state was minted for a different CMS — discarding"
        )
        return False
    expires_at = state.get("expires_at") or ""
    dt = _parse_expires_at(str(expires_at))
    if dt is None:
        return False
    remaining = dt.timestamp() - now_unix
    if remaining <= JWT_EARLY_REFRESH_SEC:
        logger.info(
            "Cached WPS JWT expires in %.0fs — will refresh", remaining
        )
        return False
    return True


def _state_from_token(
    *, cms_api_base: str, device_id: str, token: ConnectTokenResult
) -> dict:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "cms_api_base": cms_api_base,
        "device_id": device_id,
        "wps_url": token.wps_url,
        "wps_jwt": token.wps_jwt,
        "expires_at": token.expires_at,
    }


# ---------------------------------------------------------------------
# Identity loader
# ---------------------------------------------------------------------


def ensure_identity(
    *,
    device_key_path: Path,
    pairing_secret_path: Path,
) -> tuple[DeviceIdentity, str]:
    """Load (or first-boot generate) the keypair and pairing secret.

    Returns ``(identity, pairing_secret)``.  The pairing secret is needed
    by :func:`register_once` for the fleet-HMAC canonical input and
    should be treated as a low-entropy shared secret with the admin who
    scans the QR code.
    """
    identity = load_or_create_device_identity(device_key_path)
    secret = load_or_create_pairing_secret(pairing_secret_path)
    return identity, secret


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


async def ensure_wps_credentials(
    session: Any,  # aiohttp.ClientSession (kept untyped to avoid hard dep at module import)
    *,
    cms_api_base: str,
    device_id: str,
    identity: DeviceIdentity,
    pairing_secret: str,
    state_path: Path,
    fleet_id: str,
    fleet_secret: bytes,
    metadata: Optional[dict] = None,
    poll_cancel_event: Optional[asyncio.Event] = None,
    now_unix: Optional[float] = None,
) -> BootstrapCredentials:
    """Return a valid ``BootstrapCredentials`` for opening a WPS connection.

    Branches:

    1. Cached state exists and JWT not within
       :data:`JWT_EARLY_REFRESH_SEC` of expiry → return cached.
    2. Cached state exists but JWT is stale → signed ``/connect-token``,
       persist fresh state.
    3. No cached state (or cache was for a different CMS URL) → first
       boot: ``/register`` then poll ``/bootstrap-status`` until adopted,
       decrypt the outbox, then signed ``/connect-token``.

    :param session: an open ``aiohttp.ClientSession``.
    :param cms_api_base: CMS HTTP(S) base, e.g. ``"https://cms.example.com"``.
    :param device_id: the advisory device_id used at ``/register``.
        Must be the same value the device used at prior adopt time.
    :param identity: already-loaded :class:`DeviceIdentity`.
    :param pairing_secret: raw pairing secret string.
    :param state_path: path to ``bootstrap_state.json``.
    :param fleet_id: fleet identifier (empty string OK iff the device is
        already adopted — we'll never call ``/register``).
    :param fleet_secret: raw fleet HMAC key (empty bytes OK iff already
        adopted).
    :param metadata: optional advisory metadata dict for ``/register``.
    :param poll_cancel_event: optional ``asyncio.Event`` — if set during
        first-boot polling, the loop stops promptly and raises
        :class:`BootstrapCancelledError`.
    :param now_unix: clock override for tests.

    :raises BootstrapConfigError: first-boot path entered with empty
        fleet credentials.
    :raises BootstrapCancelledError: first-boot poll was cancelled.
    """
    now = now_unix if now_unix is not None else time.time()
    cached = load_state(state_path)

    # Fast path: cached + fresh.
    if cached and _is_state_fresh(cached, cms_api_base, now):
        return BootstrapCredentials(
            wps_url=str(cached["wps_url"]),
            wps_jwt=str(cached["wps_jwt"]),
            expires_at=str(cached["expires_at"]),
            device_id=str(cached.get("device_id") or device_id),
        )

    # Medium path: cached state for this CMS, JWT just stale — signed refresh.
    if cached and cached.get("cms_api_base") == cms_api_base:
        logger.info("Refreshing WPS JWT via signed /connect-token (cached state)")
        token = await fetch_connect_token(
            session,
            cms_api_base,
            device_id=str(cached.get("device_id") or device_id),
            seed=identity.seed,
        )
        state = _state_from_token(
            cms_api_base=cms_api_base,
            device_id=str(cached.get("device_id") or device_id),
            token=token,
        )
        save_state(state_path, state)
        return BootstrapCredentials(
            wps_url=token.wps_url,
            wps_jwt=token.wps_jwt,
            expires_at=token.expires_at,
            device_id=str(cached.get("device_id") or device_id),
        )

    # Slow path: first boot (or state was for a different CMS).
    if not fleet_id or not fleet_secret:
        raise BootstrapConfigError(
            "bootstrap v2 first-boot path requires AGORA_FLEET_ID and "
            "AGORA_FLEET_SECRET_HEX to be set"
        )

    logger.info("First-boot bootstrap: registering with CMS %s", cms_api_base)
    await register_once(
        session,
        cms_api_base,
        device_id=device_id,
        pubkey_b64=identity.pubkey_b64,
        pairing_secret_hash=pairing_secret_hash_hex(pairing_secret),
        fleet_id=fleet_id,
        fleet_secret=fleet_secret,
        metadata=metadata,
    )
    logger.info("Registered; polling /bootstrap-status until adopted")

    await _poll_until_adopted(
        session,
        cms_api_base=cms_api_base,
        identity=identity,
        pairing_secret=pairing_secret,
        fleet_id=fleet_id,
        fleet_secret=fleet_secret,
        device_id=device_id,
        metadata=metadata,
        poll_cancel_event=poll_cancel_event,
    )
    # _poll_until_adopted returned without raising → device is adopted
    # and we know the decrypted device_id from the outbox.
    status = await get_bootstrap_status_once(
        session, cms_api_base, pubkey_b64=identity.pubkey_b64,
    )
    if not status.is_adopted or not status.payload_b64:
        # Extremely unlikely — we just saw adopted in the loop and the
        # CMS doesn't unadopt.  Treat as transport anomaly.
        raise BootstrapTransportError(
            "/bootstrap-status flipped from adopted to non-adopted"
        )
    plaintext = decrypt_adopt_payload(identity.seed, status.payload_b64)
    try:
        payload = json.loads(plaintext.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise BootstrapTransportError(f"adopt payload was not JSON: {e!r}") from e
    adopted_device_id = str(payload.get("device_id") or device_id)

    logger.info("Adopted; minting WPS JWT via /connect-token")
    token = await fetch_connect_token(
        session, cms_api_base, device_id=adopted_device_id, seed=identity.seed,
    )
    state = _state_from_token(
        cms_api_base=cms_api_base, device_id=adopted_device_id, token=token,
    )
    save_state(state_path, state)
    return BootstrapCredentials(
        wps_url=token.wps_url,
        wps_jwt=token.wps_jwt,
        expires_at=token.expires_at,
        device_id=adopted_device_id,
    )


async def _poll_until_adopted(
    session: Any,
    *,
    cms_api_base: str,
    identity: DeviceIdentity,
    pairing_secret: str,
    fleet_id: str,
    fleet_secret: bytes,
    device_id: str,
    metadata: Optional[dict],
    poll_cancel_event: Optional[asyncio.Event],
) -> None:
    """Poll /bootstrap-status until the operator adopts.

    Handles transient errors with local backoff.  If the pending row was
    reaped (404), re-register and continue polling.
    """
    poll_count = 0
    while True:
        if poll_cancel_event is not None and poll_cancel_event.is_set():
            raise BootstrapCancelledError("first-boot poll cancelled")
        try:
            status = await get_bootstrap_status_once(
                session, cms_api_base, pubkey_b64=identity.pubkey_b64,
            )
            if status.is_adopted:
                return
        except PendingNotFoundError:
            # Pending row was reaped by the CMS garbage collector.  Re-register.
            logger.info("Pending row reaped — re-registering")
            try:
                await register_once(
                    session,
                    cms_api_base,
                    device_id=device_id,
                    pubkey_b64=identity.pubkey_b64,
                    pairing_secret_hash=pairing_secret_hash_hex(pairing_secret),
                    fleet_id=fleet_id,
                    fleet_secret=fleet_secret,
                    metadata=metadata,
                )
            except PubkeyMismatchError:
                # Extremely unlikely (row just got reaped).  Give up and
                # let the caller handle it.
                raise
            except (RateLimitedError, BootstrapTransportError, BootstrapServerError):
                pass  # fall through to sleep
        except RateLimitedError:
            pass  # sleep below
        except BootstrapTransportError:
            pass  # sleep below
        except BootstrapServerError as e:
            # 400 / 500 — log and keep trying; the CMS may be restarting.
            logger.warning("/bootstrap-status returned %s — retrying", e.status)

        delay = (
            FIRST_BOOT_POLL_SHORT_SEC
            if poll_count < FIRST_BOOT_POLL_SHORT_COUNT
            else FIRST_BOOT_POLL_LONG_SEC
        )
        poll_count += 1
        # Cooperatively wait so external cancellers wake us.
        if poll_cancel_event is not None:
            try:
                await asyncio.wait_for(poll_cancel_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            else:
                raise BootstrapCancelledError("first-boot poll cancelled")
        else:
            await asyncio.sleep(delay)


async def refresh_wps_jwt(
    session: Any,
    *,
    cms_api_base: str,
    identity: DeviceIdentity,
    state_path: Path,
    device_id: Optional[str] = None,
) -> BootstrapCredentials:
    """Mint a fresh WPS JWT via signed ``/connect-token`` and persist it.

    Used by the background renewal task.  Assumes the device is already
    adopted; if it isn't, :class:`ConnectTokenRejectedError` (401) bubbles
    out and the caller decides policy (retry with backoff, clear state
    after repeated failures, etc.).

    If ``device_id`` is omitted, the cached state's ``device_id`` is used.
    If no cached state exists, this function raises ``LookupError`` — the
    renewal task should never be reachable in that case.

    :raises ConnectTokenRejectedError: CMS returned 401.  Terminal for
        this JWT (do NOT retry with the same nonce), but the caller's
        policy decides whether to clear state.
    :raises RateLimitedError: CMS returned 429.
    :raises BootstrapTransportError: network-layer failure.
    """
    cached = load_state(state_path)
    effective_device_id = device_id
    if effective_device_id is None:
        if cached is None:
            raise LookupError(
                "refresh_wps_jwt called with no cached state and no device_id"
            )
        effective_device_id = str(cached.get("device_id") or "")
        if not effective_device_id:
            raise LookupError(
                "cached bootstrap state has no device_id — cannot refresh"
            )

    token = await fetch_connect_token(
        session,
        cms_api_base,
        device_id=effective_device_id,
        seed=identity.seed,
    )
    state = _state_from_token(
        cms_api_base=cms_api_base,
        device_id=effective_device_id,
        token=token,
    )
    save_state(state_path, state)
    return BootstrapCredentials(
        wps_url=token.wps_url,
        wps_jwt=token.wps_jwt,
        expires_at=token.expires_at,
        device_id=effective_device_id,
    )


# Re-export common error types so callers don't need to know the
# underlying module layout.
__all__ = [
    "BootstrapCancelledError",
    "BootstrapConfigError",
    "BootstrapCredentials",
    "BootstrapServerError",
    "BootstrapTransportError",
    "ConnectTokenRejectedError",
    "JWT_EARLY_REFRESH_SEC",
    "PendingNotFoundError",
    "PubkeyMismatchError",
    "RateLimitedError",
    "clear_state",
    "ensure_identity",
    "ensure_wps_credentials",
    "load_state",
    "refresh_wps_jwt",
    "save_state",
]
