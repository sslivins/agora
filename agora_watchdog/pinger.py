"""Open ``/dev/watchdog0``, set its timeout, and pet it forever.

Implemented as a small class (``Pinger``) so the run loop can be unit-tested
with injected ``time.sleep`` / ``time.monotonic`` and a tempfile for the
device. The module-level ``main()`` is what the systemd unit calls.

Kernel reference: ``Documentation/watchdog/watchdog-api.rst``.
- Writing **any** byte (other than the magic close character) pets the
  watchdog.
- Writing ``'V'`` before close requests magic-close — the kernel then
  disables the watchdog on close. Without magic-close, modern kernels
  keep the watchdog running after close (NOWAYOUT semantics), which is
  the safer default but makes a clean ``systemctl stop`` look like a
  crash to the kernel.
- ``WDIOC_SETTIMEOUT`` (ioctl _IOWR('W', 6, int)) sets the timeout in
  whole seconds. The kernel may clamp it; we ignore the kernel's
  preference and just trust it.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import struct
import sys
import time
from typing import Callable, Iterable, Optional

try:
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - exercised only on Windows test hosts
    # ``fcntl`` is Unix-only. The whole point of the IO-callable injection
    # in ``Pinger.__init__`` is that tests never actually call ``ioctl``.
    # On Windows test hosts we substitute a stub so ``import agora_watchdog``
    # succeeds; if anyone tries to use the real default at runtime they'll
    # get a clear error.
    class _FcntlStub:
        @staticmethod
        def ioctl(*_a, **_kw):
            raise RuntimeError(
                "fcntl.ioctl is not available on this platform; "
                "Pinger needs to be constructed with an explicit ioctl= "
                "callable on non-Unix hosts"
            )

    _fcntl = _FcntlStub()  # type: ignore[assignment]

log = logging.getLogger(__name__)

# ─── Kernel ioctl numbers (linux/watchdog.h) ───
# These are fixed across architectures since they're _IOR/_IOW macros over
# a 32-bit int, and the layout is identical on arm64 and x86_64.
WDIOC_KEEPALIVE = 0x80045705  # _IOR('W', 5, int)
WDIOC_SETTIMEOUT = 0xC0045706  # _IOWR('W', 6, int)
WDIOC_GETTIMEOUT = 0x80045707  # _IOR('W', 7, int)

# ─── Defaults (per Decision #16) ───
DEFAULT_DEVICE = "/dev/watchdog0"
DEFAULT_TIMEOUT_S = 12
DEFAULT_PING_INTERVAL_S = 5.0

MAGIC_CLOSE = b"V"
KEEPALIVE_BYTE = b"\0"

# ``os.O_CLOEXEC`` is Unix-only. We always want it on Linux (so a child
# fork doesn't inherit our watchdog fd) but fall back to 0 on Windows so
# the module imports cleanly for tests.
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)

# Smallest sleep slice used by ``Pinger.run`` so the loop can react to a
# stop request without waiting a full ping interval.
_TICK_S = 0.5


class WatchdogError(RuntimeError):
    """Raised when the watchdog device can't be opened or configured."""


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise WatchdogError(f"{name}={raw!r} is not an integer") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise WatchdogError(f"{name}={raw!r} is not a float") from exc


class Pinger:
    """Owns one open file descriptor on a watchdog device.

    Constructor params:

        device:        path to the watchdog character device.
        timeout_s:     value passed to ``WDIOC_SETTIMEOUT``.
        interval_s:    seconds between keepalive writes.
        opener:        injectable ``os.open``-compatible callable; the
                       test suite passes a stub that opens a regular file
                       so ``fcntl.ioctl`` is the only call that needs to
                       be patched.
        ioctl:         injectable ``fcntl.ioctl``-compatible callable.
        writer:        injectable ``os.write``-compatible callable.
        closer:        injectable ``os.close``-compatible callable.
        sleeper:       injectable ``time.sleep`` (lets tests drive the
                       loop deterministically).
    """

    def __init__(
        self,
        device: str = DEFAULT_DEVICE,
        timeout_s: int = DEFAULT_TIMEOUT_S,
        interval_s: float = DEFAULT_PING_INTERVAL_S,
        *,
        opener: Callable[[str, int], int] = os.open,
        ioctl: Callable[..., int] = _fcntl.ioctl,
        writer: Callable[[int, bytes], int] = os.write,
        closer: Callable[[int], None] = os.close,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_s <= 0:
            raise WatchdogError(f"timeout_s must be > 0, got {timeout_s!r}")
        if interval_s <= 0:
            raise WatchdogError(f"interval_s must be > 0, got {interval_s!r}")
        if interval_s >= timeout_s:
            # Petting once per timeout window is the absolute minimum;
            # anything slower guarantees a spurious reset.
            raise WatchdogError(
                f"interval_s ({interval_s}) must be strictly less than "
                f"timeout_s ({timeout_s}) to avoid spurious resets"
            )
        self.device = device
        self.timeout_s = timeout_s
        self.interval_s = interval_s
        self._opener = opener
        self._ioctl = ioctl
        self._writer = writer
        self._closer = closer
        self._sleeper = sleeper
        self._fd: Optional[int] = None
        self._stop = False
        self.ping_count = 0  # exposed for tests / future metrics

    # ── lifecycle ───────────────────────────────────────────────
    def open(self) -> None:
        """Open the device and apply ``WDIOC_SETTIMEOUT``."""
        if self._fd is not None:
            return
        try:
            self._fd = self._opener(self.device, os.O_WRONLY | _O_CLOEXEC)
        except OSError as exc:
            raise WatchdogError(
                f"opening watchdog device {self.device!r} failed: {exc}"
            ) from exc
        buf = struct.pack("i", int(self.timeout_s))
        try:
            self._ioctl(self._fd, WDIOC_SETTIMEOUT, buf)
        except OSError as exc:
            # Failure to set timeout is fatal — running at the kernel
            # default (often 60s) silently weakens the contract.
            try:
                self._closer(self._fd)
            finally:
                self._fd = None
            raise WatchdogError(
                f"WDIOC_SETTIMEOUT={self.timeout_s} on {self.device!r} "
                f"failed: {exc}"
            ) from exc
        log.info(
            "watchdog opened: device=%s timeout=%ds interval=%.1fs",
            self.device,
            self.timeout_s,
            self.interval_s,
        )

    def ping(self) -> None:
        """Write one keepalive byte. Raises if not opened."""
        if self._fd is None:
            raise WatchdogError("ping() called before open()")
        self._writer(self._fd, KEEPALIVE_BYTE)
        self.ping_count += 1

    def close(self, *, magic: bool = True) -> None:
        """Close the fd; if ``magic=True``, write ``'V'`` first.

        Magic-close requests that the kernel disable the watchdog when
        the fd closes (rather than continuing to count down). Used on
        graceful shutdown so ``systemctl stop`` doesn't trigger a reset
        12 seconds later.
        """
        if self._fd is None:
            return
        fd = self._fd
        self._fd = None
        try:
            if magic:
                try:
                    self._writer(fd, MAGIC_CLOSE)
                except OSError as exc:  # pragma: no cover - rare path
                    log.warning("magic-close write failed: %s", exc)
        finally:
            try:
                self._closer(fd)
            except OSError as exc:  # pragma: no cover - rare path
                log.warning("watchdog close failed: %s", exc)

    # ── control ─────────────────────────────────────────────────
    def stop(self, *_args) -> None:
        """Signal the run loop to exit on its next tick.

        Suitable as a ``signal.signal`` handler; ignores all positional
        args (signum, frame) so it can also be called by hand.
        """
        self._stop = True

    @property
    def stopped(self) -> bool:
        return self._stop

    # ── main loop ───────────────────────────────────────────────
    def run(self, *, install_signal_handlers: bool = True) -> None:
        """Pet forever. Returns when ``stop()`` is called.

        Sleeps in ``_TICK_S`` slices so a SIGTERM doesn't have to wait a
        full ping interval to be honoured.
        """
        self.open()
        prev_handlers: list[tuple[int, object]] = []
        try:
            if install_signal_handlers:
                for sig in self._installable_signals():
                    try:
                        prev = signal.signal(sig, self.stop)
                        prev_handlers.append((sig, prev))
                    except (OSError, ValueError):  # pragma: no cover
                        # ValueError on non-main thread, OSError on
                        # Windows for SIGTERM. Tests bypass this branch.
                        pass
            while not self._stop:
                self.ping()
                self._sleep_for(self.interval_s)
        finally:
            for sig, prev in prev_handlers:
                try:
                    signal.signal(sig, prev)  # type: ignore[arg-type]
                except (OSError, ValueError):  # pragma: no cover
                    pass
            self.close(magic=True)

    @staticmethod
    def _installable_signals() -> Iterable[int]:
        sigs = [signal.SIGTERM, signal.SIGINT]
        # SIGHUP isn't on Windows; skip if missing.
        sighup = getattr(signal, "SIGHUP", None)
        if sighup is not None:
            sigs.append(sighup)
        return sigs

    def _sleep_for(self, total_s: float) -> None:
        remaining = total_s
        while remaining > 0 and not self._stop:
            slice_s = remaining if remaining < _TICK_S else _TICK_S
            self._sleeper(slice_s)
            remaining -= slice_s


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entrypoint. Used by ``python3 -m agora_watchdog``.

    Exit codes:
        0 — clean shutdown via signal
        1 — open / configuration error
        2 — argparse error (handled by argparse itself)
    """
    try:
        parser = argparse.ArgumentParser(
            prog="agora-watchdog",
            description="Pet the Pi 5 hardware watchdog (/dev/watchdog0).",
        )
        parser.add_argument(
            "--device",
            default=_env("AGORA_WATCHDOG_DEVICE", DEFAULT_DEVICE),
            help=f"watchdog character device (default: {DEFAULT_DEVICE})",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=_env_int("AGORA_WATCHDOG_TIMEOUT_S", DEFAULT_TIMEOUT_S),
            help=f"WDIOC_SETTIMEOUT in seconds (default: {DEFAULT_TIMEOUT_S})",
        )
        parser.add_argument(
            "--interval",
            type=float,
            default=_env_float(
                "AGORA_WATCHDOG_PING_INTERVAL_S", DEFAULT_PING_INTERVAL_S
            ),
            help=(
                "ping cadence in seconds "
                f"(default: {DEFAULT_PING_INTERVAL_S})"
            ),
        )
        parser.add_argument(
            "-v", "--verbose", action="store_true", help="enable debug logging"
        )
        args = parser.parse_args(argv)

        logging.basicConfig(
            level=logging.DEBUG if args.verbose else logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

        pinger = Pinger(
            device=args.device,
            timeout_s=args.timeout,
            interval_s=args.interval,
        )
        pinger.run()
        return 0
    except WatchdogError as exc:
        log.error("watchdog failed: %s", exc)
        return 1
    except KeyboardInterrupt:  # pragma: no cover - kept for parity with sigint
        return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
