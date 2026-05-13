"""Unit tests for ``agora_watchdog.pinger``.

The pinger talks to ``/dev/watchdog0`` via ``os.open`` / ``fcntl.ioctl`` /
``os.write`` / ``os.close``. The ``Pinger`` constructor takes all four as
injectable callables so the tests can:

  - record the exact arguments passed to ``ioctl`` (timeout struct, ioctl
    number)
  - assert the keepalive byte vs magic-close byte are written at the
    right moments
  - drive the run loop deterministically with a fake sleeper
  - verify the loop honors ``stop()`` within one tick (not one ping
    interval)

We don't try to actually exercise ``/dev/watchdog0`` here — that's covered
in the hardware acceptance tests on the dev Pi5.
"""

from __future__ import annotations

import os
import struct
from typing import Any, List, Tuple

import pytest

from agora_watchdog import pinger as pinger_mod
from agora_watchdog.pinger import (
    DEFAULT_DEVICE,
    DEFAULT_PING_INTERVAL_S,
    DEFAULT_TIMEOUT_S,
    KEEPALIVE_BYTE,
    MAGIC_CLOSE,
    WDIOC_SETTIMEOUT,
    Pinger,
    WatchdogError,
    main,
)


class FakeWatchdog:
    """Records every interaction the Pinger has with its 'device'.

    Stands in for the four os-level callables. Each call appends a row
    to ``events`` so a test can assert ordering (open → ioctl → write*
    → magic-close → close).
    """

    def __init__(self, *, fd: int = 99) -> None:
        self.events: List[Tuple[str, Any]] = []
        self.fd = fd
        self.opened = False
        self.closed = False
        # Allow tests to inject failures for individual operations.
        self.open_err: Exception | None = None
        self.ioctl_err: Exception | None = None
        self.write_err: Exception | None = None

    def opener(self, path: str, flags: int) -> int:
        if self.open_err is not None:
            raise self.open_err
        self.events.append(("open", (path, flags)))
        self.opened = True
        return self.fd

    def ioctl(self, fd: int, request: int, arg: Any) -> int:
        if self.ioctl_err is not None:
            raise self.ioctl_err
        self.events.append(("ioctl", (fd, request, bytes(arg))))
        return 0

    def writer(self, fd: int, data: bytes) -> int:
        if self.write_err is not None:
            raise self.write_err
        self.events.append(("write", (fd, bytes(data))))
        return len(data)

    def closer(self, fd: int) -> None:
        self.events.append(("close", fd))
        self.closed = True

    # ── convenience filters ──
    def writes(self) -> List[bytes]:
        return [args[1] for kind, args in self.events if kind == "write"]

    def ioctls(self) -> List[Tuple[int, int, bytes]]:
        return [args for kind, args in self.events if kind == "ioctl"]

    def kinds(self) -> List[str]:
        return [kind for kind, _ in self.events]


@pytest.fixture
def fake() -> FakeWatchdog:
    return FakeWatchdog()


def _make_pinger(
    fake: FakeWatchdog,
    *,
    device: str = "/dev/watchdog0",
    timeout_s: int = 12,
    interval_s: float = 5.0,
    sleeper=lambda _s: None,
) -> Pinger:
    return Pinger(
        device=device,
        timeout_s=timeout_s,
        interval_s=interval_s,
        opener=fake.opener,
        ioctl=fake.ioctl,
        writer=fake.writer,
        closer=fake.closer,
        sleeper=sleeper,
    )


# ─── constants ─────────────────────────────────────────────────


def test_defaults_match_decision_16():
    """Decision #16: 12s timeout, 5s ping, /dev/watchdog0."""
    assert DEFAULT_DEVICE == "/dev/watchdog0"
    assert DEFAULT_TIMEOUT_S == 12
    assert DEFAULT_PING_INTERVAL_S == 5.0


def test_wdioc_settimeout_constant():
    """The ioctl number is fixed by the kernel; pin it so we notice if
    someone "fixes" the hex value."""
    assert WDIOC_SETTIMEOUT == 0xC0045706


def test_magic_close_byte_is_capital_v():
    assert MAGIC_CLOSE == b"V"


def test_keepalive_byte_is_not_v():
    """Any byte other than 'V' pets the watchdog. We pick NUL to avoid
    any chance of accidentally writing the magic-close byte in the
    keepalive path."""
    assert KEEPALIVE_BYTE != MAGIC_CLOSE


# ─── constructor validation ────────────────────────────────────


@pytest.mark.parametrize("bad", [0, -1, -100])
def test_constructor_rejects_nonpositive_timeout(bad, fake):
    with pytest.raises(WatchdogError, match="timeout_s"):
        _make_pinger(fake, timeout_s=bad)


@pytest.mark.parametrize("bad", [0, -0.5, -10])
def test_constructor_rejects_nonpositive_interval(bad, fake):
    with pytest.raises(WatchdogError, match="interval_s"):
        _make_pinger(fake, interval_s=bad)


def test_constructor_rejects_interval_geq_timeout(fake):
    """An interval >= timeout guarantees a spurious reset because the
    kernel decrements the timer between writes."""
    with pytest.raises(WatchdogError, match="strictly less"):
        _make_pinger(fake, timeout_s=12, interval_s=12)
    with pytest.raises(WatchdogError, match="strictly less"):
        _make_pinger(fake, timeout_s=12, interval_s=15)


# ─── open() ────────────────────────────────────────────────────


def test_open_sends_settimeout_ioctl(fake):
    p = _make_pinger(fake, timeout_s=12)
    p.open()
    # First two events: open, then ioctl(WDIOC_SETTIMEOUT, packed 12s)
    assert fake.kinds()[:2] == ["open", "ioctl"]
    fd, req, arg = fake.ioctls()[0]
    assert fd == fake.fd
    assert req == WDIOC_SETTIMEOUT
    assert struct.unpack("i", arg)[0] == 12


def test_open_passes_o_wronly_and_cloexec(fake):
    p = _make_pinger(fake)
    p.open()
    (_path, flags) = fake.events[0][1]
    assert flags & os.O_WRONLY
    # O_CLOEXEC is Unix-only; on Windows test hosts the flag is 0.
    if pinger_mod._O_CLOEXEC:
        assert flags & pinger_mod._O_CLOEXEC


def test_open_is_idempotent(fake):
    """Calling open() twice doesn't reopen or re-ioctl."""
    p = _make_pinger(fake)
    p.open()
    p.open()
    assert fake.kinds().count("open") == 1
    assert fake.kinds().count("ioctl") == 1


def test_open_failure_raises_watchdogerror(fake):
    fake.open_err = PermissionError("EACCES")
    p = _make_pinger(fake)
    with pytest.raises(WatchdogError, match="opening watchdog"):
        p.open()
    # We never recorded the open event since the stub raised before append.
    assert "ioctl" not in fake.kinds()


def test_ioctl_failure_closes_fd_and_raises(fake):
    """If WDIOC_SETTIMEOUT fails we must release the fd; otherwise a
    second start would leak."""
    fake.ioctl_err = OSError("EINVAL")
    p = _make_pinger(fake)
    with pytest.raises(WatchdogError, match="WDIOC_SETTIMEOUT"):
        p.open()
    # We did open, then attempted ioctl (which raised), then closed.
    assert "close" in fake.kinds()


# ─── ping() ────────────────────────────────────────────────────


def test_ping_writes_keepalive_byte(fake):
    p = _make_pinger(fake)
    p.open()
    p.ping()
    assert fake.writes() == [KEEPALIVE_BYTE]
    assert p.ping_count == 1


def test_ping_before_open_raises(fake):
    p = _make_pinger(fake)
    with pytest.raises(WatchdogError, match="before open"):
        p.ping()


def test_multiple_pings_accumulate_count(fake):
    p = _make_pinger(fake)
    p.open()
    for _ in range(5):
        p.ping()
    assert p.ping_count == 5
    assert fake.writes() == [KEEPALIVE_BYTE] * 5


# ─── close() ───────────────────────────────────────────────────


def test_close_writes_magic_close_by_default(fake):
    p = _make_pinger(fake)
    p.open()
    p.close()
    # Last write before close() must be 'V'.
    assert fake.writes()[-1] == MAGIC_CLOSE
    assert "close" in fake.kinds()


def test_close_magic_false_skips_magic_byte(fake):
    p = _make_pinger(fake)
    p.open()
    p.close(magic=False)
    assert MAGIC_CLOSE not in fake.writes()
    assert "close" in fake.kinds()


def test_close_is_idempotent(fake):
    p = _make_pinger(fake)
    p.open()
    p.close()
    p.close()  # second call is a no-op
    assert fake.kinds().count("close") == 1


# ─── run() ─────────────────────────────────────────────────────


def test_run_pings_then_stops_cleanly(fake):
    """Drive run(): inject a fake sleeper that stops the pinger after
    the third ping, confirm we got exactly three keepalives followed by
    the magic-close byte."""
    pings_seen: List[int] = []
    p = _make_pinger(fake, interval_s=5.0)

    def fake_sleep(_dt: float) -> None:
        pings_seen.append(p.ping_count)
        if p.ping_count >= 3:
            p.stop()

    p._sleeper = fake_sleep  # type: ignore[attr-defined]
    p.run(install_signal_handlers=False)

    # Three keepalive writes (one per ping), followed by the magic-close.
    keepalives = [w for w in fake.writes() if w == KEEPALIVE_BYTE]
    assert len(keepalives) == 3
    assert fake.writes()[-1] == MAGIC_CLOSE


def test_run_stop_honors_tick_granularity(fake):
    """Even with interval_s=60, calling stop() should exit within a
    single _TICK_S window rather than waiting a full minute."""
    sleep_calls: List[float] = []
    p = _make_pinger(fake, timeout_s=120, interval_s=60.0)

    def fake_sleep(dt: float) -> None:
        sleep_calls.append(dt)
        # Stop after the very first sleep slice.
        p.stop()

    p._sleeper = fake_sleep  # type: ignore[attr-defined]
    p.run(install_signal_handlers=False)

    # Should have slept exactly one _TICK_S slice (0.5s) and exited,
    # rather than the full 60s interval.
    assert sleep_calls == [pinger_mod._TICK_S]


def test_run_writes_magic_close_on_stop(fake):
    p = _make_pinger(fake)
    p._sleeper = lambda _dt: p.stop()  # type: ignore[attr-defined]
    p.run(install_signal_handlers=False)
    # Final byte written must be magic-close so the kernel disables the
    # watchdog instead of resetting the box after timeout seconds.
    assert fake.writes()[-1] == MAGIC_CLOSE


def test_run_opens_before_first_ping(fake):
    """run() must call open() (and therefore ioctl) before the first
    write — otherwise a slow kernel + fast first-write race could pet
    at the wrong timeout."""
    p = _make_pinger(fake)
    p._sleeper = lambda _dt: p.stop()  # type: ignore[attr-defined]
    p.run(install_signal_handlers=False)
    kinds = fake.kinds()
    # The first three kinds must be: open, ioctl, write.
    assert kinds[:3] == ["open", "ioctl", "write"]


# ─── main() / env vars ─────────────────────────────────────────


def test_main_env_overrides(fake, monkeypatch):
    """AGORA_WATCHDOG_* env vars should reach the Pinger constructor."""
    captured: dict = {}

    class Recorded(Pinger):
        def __init__(self, *a, **kw):
            captured.update(kw)
            captured["args"] = a
            super().__init__(
                *a,
                **kw,
                opener=fake.opener,
                ioctl=fake.ioctl,
                writer=fake.writer,
                closer=fake.closer,
                sleeper=lambda _dt: self.stop(),
            )

    monkeypatch.setattr(pinger_mod, "Pinger", Recorded)
    monkeypatch.setenv("AGORA_WATCHDOG_DEVICE", "/tmp/wd-fake")
    monkeypatch.setenv("AGORA_WATCHDOG_TIMEOUT_S", "8")
    monkeypatch.setenv("AGORA_WATCHDOG_PING_INTERVAL_S", "2.5")

    rc = main([])
    assert rc == 0
    assert captured["device"] == "/tmp/wd-fake"
    assert captured["timeout_s"] == 8
    assert captured["interval_s"] == 2.5


def test_main_cli_overrides_env(fake, monkeypatch):
    """CLI flags should beat env vars."""
    captured: dict = {}

    class Recorded(Pinger):
        def __init__(self, *a, **kw):
            captured.update(kw)
            super().__init__(
                *a,
                **kw,
                opener=fake.opener,
                ioctl=fake.ioctl,
                writer=fake.writer,
                closer=fake.closer,
                sleeper=lambda _dt: self.stop(),
            )

    monkeypatch.setattr(pinger_mod, "Pinger", Recorded)
    monkeypatch.setenv("AGORA_WATCHDOG_TIMEOUT_S", "8")
    rc = main(["--timeout", "10", "--interval", "3"])
    assert rc == 0
    assert captured["timeout_s"] == 10
    assert captured["interval_s"] == 3.0


def test_main_returns_1_on_watchdog_error(monkeypatch):
    def boom(*_a, **_kw):
        raise WatchdogError("nope")

    monkeypatch.setattr(pinger_mod, "Pinger", boom)
    rc = main([])
    assert rc == 1


@pytest.mark.parametrize(
    "var,bad_value",
    [
        ("AGORA_WATCHDOG_TIMEOUT_S", "abc"),
        ("AGORA_WATCHDOG_PING_INTERVAL_S", "five"),
    ],
)
def test_main_returns_1_on_invalid_env(monkeypatch, var, bad_value):
    """Non-numeric env values should produce WatchdogError → exit 1,
    not a Python traceback."""
    monkeypatch.setenv(var, bad_value)
    rc = main([])
    assert rc == 1


# ─── package re-exports ────────────────────────────────────────


def test_package_reexports():
    """The top-level package surface is what other code (and the
    systemd unit) imports. Don't break it accidentally."""
    import agora_watchdog

    assert agora_watchdog.DEFAULT_DEVICE == DEFAULT_DEVICE
    assert agora_watchdog.DEFAULT_TIMEOUT_S == DEFAULT_TIMEOUT_S
    assert agora_watchdog.DEFAULT_PING_INTERVAL_S == DEFAULT_PING_INTERVAL_S
    assert agora_watchdog.MAGIC_CLOSE == MAGIC_CLOSE
    assert agora_watchdog.Pinger is Pinger
    assert agora_watchdog.WatchdogError is WatchdogError
    assert callable(agora_watchdog.main)
