"""Unit tests for ``slot_mgr.autoboot``: parser round-trips and section edits."""

from __future__ import annotations

import textwrap

import pytest

from slot_mgr import autoboot as ab


SAMPLE = textwrap.dedent("""\
    # autoboot.txt - Pi 5 A/B boot configuration for agora.
    # Managed by agora-slot-mgr; mirrored on both boot-A and boot-B.

    [all]
    tryboot_a_b=1
    boot_partition=1

    [tryboot]
    boot_partition=3
""")


class TestParse:
    def test_round_trip_preserves_bytes(self):
        """Parsing and re-emitting yields the same content."""
        parsed = ab.parse_autoboot(SAMPLE)
        assert parsed.to_string() == SAMPLE

    def test_read_default_and_tryboot_partitions(self):
        parsed = ab.parse_autoboot(SAMPLE)
        assert parsed.default_partition() == ab.PART_BOOT_A
        assert parsed.tryboot_partition() == ab.PART_BOOT_B

    def test_default_slot_maps_partition_to_slot_number(self):
        parsed = ab.parse_autoboot(SAMPLE)
        assert parsed.default_slot() == ab.SLOT_A

    def test_get_returns_none_for_unknown_key(self):
        parsed = ab.parse_autoboot(SAMPLE)
        assert parsed.get("all", "no_such_key") is None
        assert parsed.get("nonexistent", "boot_partition") is None

    def test_preserves_comments(self):
        parsed = ab.parse_autoboot(SAMPLE)
        emitted = parsed.to_string()
        assert "# autoboot.txt" in emitted
        assert "# Managed by agora-slot-mgr" in emitted

    def test_implicit_section_key_rejected(self):
        """A kv line before any [section] header is a malformed file we refuse."""
        bad = "boot_partition=1\n[all]\n"
        with pytest.raises(ab.AutobootError):
            ab.parse_autoboot(bad)

    def test_handles_missing_trailing_newline(self):
        no_newline = "[all]\nboot_partition=1"
        parsed = ab.parse_autoboot(no_newline)
        # Re-emit without forcing a trailing newline
        assert parsed.to_string() == no_newline

    def test_trailing_newline_is_preserved_on_round_trip(self):
        with_newline = "[all]\nboot_partition=1\n"
        parsed = ab.parse_autoboot(with_newline)
        assert parsed.to_string() == with_newline


class TestSetDefaultPartition:
    def test_mutates_existing_value_only(self):
        parsed = ab.parse_autoboot(SAMPLE)
        parsed.set_default_partition(ab.PART_BOOT_B)
        assert parsed.default_partition() == ab.PART_BOOT_B
        # tryboot must NOT change
        assert parsed.tryboot_partition() == ab.PART_BOOT_B

    def test_emit_after_mutation_keeps_structure(self):
        parsed = ab.parse_autoboot(SAMPLE)
        parsed.set_default_partition(ab.PART_BOOT_B)
        emitted = parsed.to_string()
        # The [all] block still exists; the value flipped
        assert "[all]" in emitted
        assert "boot_partition=3" in emitted
        # Comment block still intact
        assert "# autoboot.txt" in emitted

    def test_inserts_missing_key(self):
        """If [all] exists but lacks boot_partition we add it inside [all]."""
        text = "[all]\ntryboot_a_b=1\n\n[tryboot]\nboot_partition=3\n"
        parsed = ab.parse_autoboot(text)
        parsed.set_default_partition(ab.PART_BOOT_A)
        emitted = parsed.to_string()
        # boot_partition must land between [all] and [tryboot]
        all_idx = emitted.index("[all]")
        tryboot_idx = emitted.index("[tryboot]")
        bp_idx = emitted.index("boot_partition=1")
        assert all_idx < bp_idx < tryboot_idx

    def test_appends_section_if_missing(self):
        text = "[tryboot]\nboot_partition=3\n"
        parsed = ab.parse_autoboot(text)
        parsed.set_default_partition(ab.PART_BOOT_A)
        emitted = parsed.to_string()
        assert "[all]" in emitted
        assert "boot_partition=1" in emitted
        # Original [tryboot] line preserved
        assert "boot_partition=3" in emitted


class TestSetTrybootPartition:
    def test_can_flip_tryboot_target(self):
        parsed = ab.parse_autoboot(SAMPLE)
        parsed.set_tryboot_partition(ab.PART_BOOT_A)
        assert parsed.tryboot_partition() == ab.PART_BOOT_A
        # [all] unchanged
        assert parsed.default_partition() == ab.PART_BOOT_A


class TestReadWrite:
    def test_write_creates_file_atomically(self, tmp_path):
        target = tmp_path / "boot" / "firmware" / "autoboot.txt"
        ab.write_autoboot(ab.parse_autoboot(SAMPLE), target)
        assert target.read_text() == SAMPLE

    def test_write_with_mirror_writes_both(self, tmp_path):
        primary = tmp_path / "boot-A" / "autoboot.txt"
        mirror = tmp_path / "boot-B" / "autoboot.txt"
        primary.parent.mkdir(parents=True)
        mirror.parent.mkdir(parents=True)
        ab.write_autoboot(ab.parse_autoboot(SAMPLE), primary, mirrors=[mirror])
        # The whole point of mirroring is that the two on-disk copies are
        # byte-identical (so the bootloader sees the same autoboot.txt no
        # matter which boot partition it lands on). We deliberately do NOT
        # compare against ``SAMPLE.encode()`` here: text-mode write on
        # Windows substitutes ``\r\n`` for ``\n``, but the primary/mirror
        # pair is still identical to each other, which is the invariant
        # the production code is responsible for.
        assert primary.read_bytes() == mirror.read_bytes()
        # And the *content* still round-trips through the parser.
        assert ab.read_autoboot(primary).default_partition() == ab.PART_BOOT_A

    def test_read_autoboot_roundtrips_from_disk(self, tmp_path):
        target = tmp_path / "autoboot.txt"
        target.write_text(SAMPLE)
        parsed = ab.read_autoboot(target)
        assert parsed.default_partition() == ab.PART_BOOT_A
        assert parsed.to_string() == SAMPLE

    def test_mirror_failure_does_not_corrupt_primary(self, tmp_path):
        """If the mirror path can't be written (parent missing), the primary
        write has already committed when the mirror attempt raises."""
        primary = tmp_path / "primary" / "autoboot.txt"
        primary.parent.mkdir(parents=True)
        # Mirror parent does not exist and is a *file* not a directory, so
        # mkdir() raises a NotADirectoryError. Verify the primary already
        # contains the new content before the exception bubbles.
        blocker = tmp_path / "blocker"
        blocker.write_text("not a directory")
        mirror = blocker / "subdir" / "autoboot.txt"

        with pytest.raises((NotADirectoryError, FileExistsError, OSError)):
            ab.write_autoboot(ab.parse_autoboot(SAMPLE), primary, mirrors=[mirror])
        # Primary committed regardless
        assert primary.read_text() == SAMPLE
