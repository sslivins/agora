"""Unit tests for the ``agora-slot-mgr`` CLI verb dispatch."""

from __future__ import annotations

import json
import textwrap

import pytest

from slot_mgr import autoboot as ab
from slot_mgr import cli
from slot_mgr.state import STRIKE_LIMIT, load_state


AUTOBOOT_DEFAULT = textwrap.dedent("""\
    # managed by agora-slot-mgr
    [all]
    tryboot_a_b=1
    boot_partition=1

    [tryboot]
    boot_partition=3
""")


@pytest.fixture
def slot_env(tmp_path, monkeypatch):
    """Same idea as test_slot_mgr_core but a slimmer setup."""
    autoboot = tmp_path / "boot" / "autoboot.txt"
    mirror = tmp_path / "boot-b" / "autoboot.txt"
    data = tmp_path / "data"
    cmdline = tmp_path / "cmdline"

    autoboot.parent.mkdir()
    mirror.parent.mkdir()
    data.mkdir()
    autoboot.write_text(AUTOBOOT_DEFAULT)
    mirror.write_text(AUTOBOOT_DEFAULT)
    cmdline.write_text("root=PARTLABEL=root-A rootwait\n")

    monkeypatch.setenv("AGORA_AUTOBOOT_PATH", str(autoboot))
    monkeypatch.setenv("AGORA_AUTOBOOT_MIRROR_PATH", str(mirror))
    monkeypatch.setenv("AGORA_SLOT_DATA_DIR", str(data))
    monkeypatch.setenv("AGORA_PROC_CMDLINE", str(cmdline))

    return {"autoboot": autoboot, "mirror": mirror, "data": data, "cmdline": cmdline}


class TestStatus:
    def test_status_human_output(self, slot_env, capsys):
        rc = cli.main(["status"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "running slot:" in out
        assert "default slot:" in out
        assert "tentative tryboot:" in out
        assert "strikes:" in out

    def test_status_json_output(self, slot_env, capsys):
        rc = cli.main(["status", "--json"])
        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["running_slot"] == 1
        assert payload["default_slot"] == 1
        assert payload["tentative"] is False
        assert payload["pinned"] is False
        assert payload["strikes"] == {"1": 0, "2": 0}


class TestTrybootVerb:
    def test_no_reboot_dispatches_correctly(self, slot_env, capsys, monkeypatch):
        rebooted = []
        monkeypatch.setattr(
            "slot_mgr.core._default_reboot",
            lambda: rebooted.append(True),
        )
        rc = cli.main(["trigger-tryboot", "2", "--no-reboot"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "staged tryboot of slot 2" in out
        assert "--no-reboot set" in out
        assert rebooted == []
        # autoboot rewritten
        assert ab.read_autoboot(slot_env["autoboot"]).tryboot_partition() == ab.PART_BOOT_B

    def test_letter_slot_accepted(self, slot_env, capsys, monkeypatch):
        monkeypatch.setattr("slot_mgr.core._default_reboot", lambda: None)
        rc = cli.main(["trigger-tryboot", "B", "--no-reboot"])
        assert rc == 0

    def test_invalid_slot_arg_returns_argparse_error(self, slot_env, capsys):
        with pytest.raises(SystemExit) as exc:
            cli.main(["trigger-tryboot", "Z", "--no-reboot"])
        # argparse exits 2 on argument errors
        assert exc.value.code == 2


class TestPromoteVerb:
    def test_promote_rewrites_all_section(self, slot_env, capsys):
        rc = cli.main(["promote", "2"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "promoted slot 2 to default" in out
        assert ab.read_autoboot(slot_env["autoboot"]).default_partition() == ab.PART_BOOT_B

    def test_promote_writes_sentinel(self, slot_env):
        cli.main(["promote", "1"])
        sentinel = slot_env["data"] / "migration-allowed"
        assert sentinel.exists()
        assert "slot=1" in sentinel.read_text()


class TestRecordStrikeVerb:
    def test_strike_increments(self, slot_env, capsys):
        rc = cli.main(["record-strike", "2"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "recorded strike for slot 2" in out
        assert load_state().get_strikes(2) == 1

    def test_strike_to_pin_then_tryboot_refused(self, slot_env, capsys, monkeypatch):
        monkeypatch.setattr("slot_mgr.core._default_reboot", lambda: None)
        for _ in range(STRIKE_LIMIT):
            cli.main(["record-strike", "2"])
        assert load_state().pinned is True

        rc = cli.main(["trigger-tryboot", "2", "--no-reboot"])
        # PinnedError exit code from cli.main
        assert rc == 3


class TestUnpinVerb:
    def test_unpin_clears_state(self, slot_env, capsys):
        for _ in range(STRIKE_LIMIT):
            cli.main(["record-strike", "1"])
        assert load_state().pinned is True

        rc = cli.main(["unpin"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "unpinned" in out
        assert load_state().pinned is False
        assert load_state().strikes == {"1": 0, "2": 0}


class TestNoVerb:
    def test_missing_verb_errors(self, slot_env):
        with pytest.raises(SystemExit) as exc:
            cli.main([])
        assert exc.value.code == 2
