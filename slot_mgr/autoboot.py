"""Parser and rewriter for Pi 5 ``autoboot.txt``.

The file is small and INI-like, but we cannot use stdlib :mod:`configparser`
because we want to *preserve* in-line comments and blank lines so the file
stays readable for whoever has to debug a brick by mounting the SD card on
their laptop.

Strategy: tokenize the file into an ordered list of ``Line`` records
(comment / blank / section header / key=value), mutate the records in
place, and emit the file by re-joining them. Unknown sections / keys are
left untouched.

The only operations slot_mgr performs are:

* ``read_autoboot`` / ``parse_autoboot`` - introspect ``[all] boot_partition``
* ``set_default_partition(part)`` - rewrite ``[all] boot_partition=N``
* ``set_tryboot_partition(part)`` - rewrite ``[tryboot] boot_partition=N``
* ``write_autoboot(...)`` - persist with byte-for-byte mirroring to boot-B

Pi 5 autoboot.txt section semantics (from the Raspberry Pi bootloader docs):
``[all]`` is the default boot config; ``[tryboot]`` is consulted for exactly
one boot when triggered via ``reboot '0 tryboot'`` or
``vcgencmd reboot_to_tryboot``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

# Partition numbers in the Phase 0 GPT layout (see plan.md).
PART_BOOT_A = 1
PART_BOOT_B = 3

# Slot numbers used at the API surface.
SLOT_A = 1
SLOT_B = 2

#: Mapping from user-facing slot number -> GPT partition number of its boot fs.
SLOT_TO_BOOT_PARTITION = {SLOT_A: PART_BOOT_A, SLOT_B: PART_BOOT_B}
BOOT_PARTITION_TO_SLOT = {v: k for k, v in SLOT_TO_BOOT_PARTITION.items()}

SECTION_ALL = "all"
SECTION_TRYBOOT = "tryboot"


class AutobootError(ValueError):
    """Raised for malformed autoboot.txt content or invalid edits."""


@dataclass
class _Line:
    """One physical line in autoboot.txt, classified."""

    kind: str  # "comment" | "blank" | "section" | "kv" | "raw"
    text: str  # original text (without trailing newline)
    section: str | None = None  # section this line belongs to
    key: str | None = None
    value: str | None = None


@dataclass
class Autoboot:
    """Parsed autoboot.txt with the ability to mutate and re-emit."""

    lines: list[_Line] = field(default_factory=list)
    trailing_newline: bool = True

    # -- introspection ------------------------------------------------------

    def get(self, section: str, key: str) -> str | None:
        """Return the value of ``key`` in ``section``, or ``None`` if absent."""
        for line in self.lines:
            if line.kind == "kv" and line.section == section and line.key == key:
                return line.value
        return None

    def default_partition(self) -> int | None:
        raw = self.get(SECTION_ALL, "boot_partition")
        return None if raw is None else int(raw)

    def tryboot_partition(self) -> int | None:
        raw = self.get(SECTION_TRYBOOT, "boot_partition")
        return None if raw is None else int(raw)

    def default_slot(self) -> int | None:
        part = self.default_partition()
        return None if part is None else BOOT_PARTITION_TO_SLOT.get(part)

    # -- mutation -----------------------------------------------------------

    def set_default_partition(self, partition: int) -> None:
        """Rewrite ``[all] boot_partition=N``, creating the section if needed."""
        self._set(SECTION_ALL, "boot_partition", str(partition))

    def set_tryboot_partition(self, partition: int) -> None:
        """Rewrite ``[tryboot] boot_partition=N``, creating the section if needed."""
        self._set(SECTION_TRYBOOT, "boot_partition", str(partition))

    def _set(self, section: str, key: str, value: str) -> None:
        for line in self.lines:
            if line.kind == "kv" and line.section == section and line.key == key:
                line.value = value
                line.text = f"{key}={value}"
                return

        # Key not present - find or create the section and append the kv at its
        # end (just before the next section header, or at the end of the file).
        section_start = self._find_section_start(section)
        if section_start is None:
            self._append_section(section)
            section_start = self._find_section_start(section)
            assert section_start is not None  # we just created it

        insert_at = self._find_section_end(section_start)
        self.lines.insert(
            insert_at,
            _Line(kind="kv", text=f"{key}={value}", section=section, key=key, value=value),
        )

    def _find_section_start(self, section: str) -> int | None:
        for idx, line in enumerate(self.lines):
            if line.kind == "section" and line.section == section:
                return idx
        return None

    def _find_section_end(self, start: int) -> int:
        """Return the insertion index for a new kv at the end of the section
        whose header is at ``start``. Walks until the next section header (or EOF).
        """
        idx = start + 1
        last_meaningful = start
        while idx < len(self.lines):
            line = self.lines[idx]
            if line.kind == "section":
                break
            if line.kind in ("kv", "comment"):
                last_meaningful = idx
            idx += 1
        return last_meaningful + 1

    def _append_section(self, section: str) -> None:
        if self.lines and self.lines[-1].kind != "blank":
            self.lines.append(_Line(kind="blank", text=""))
        self.lines.append(_Line(kind="section", text=f"[{section}]", section=section))

    # -- emit ---------------------------------------------------------------

    def to_string(self) -> str:
        body = "\n".join(line.text for line in self.lines)
        if self.trailing_newline and not body.endswith("\n"):
            body += "\n"
        return body


# -- module-level helpers ---------------------------------------------------


def parse_autoboot(text: str) -> Autoboot:
    """Tokenize raw autoboot.txt content into an :class:`Autoboot` object.

    Raises :class:`AutobootError` on a kv line that appears before any section
    header (the Pi bootloader treats those as part of ``[all]``, but we'd
    rather refuse to round-trip a file with implicit-section keys because the
    re-emit would change semantics).
    """
    lines: list[_Line] = []
    current_section: str | None = None
    raw_lines = text.split("\n")
    trailing_newline = text.endswith("\n")
    # When the file ends with a newline ``str.split`` yields an empty trailing
    # entry; drop it so we don't emit a phantom blank line on round-trip.
    if trailing_newline and raw_lines and raw_lines[-1] == "":
        raw_lines = raw_lines[:-1]

    for raw in raw_lines:
        stripped = raw.strip()
        if not stripped:
            lines.append(_Line(kind="blank", text=raw))
            continue
        if stripped.startswith("#"):
            lines.append(_Line(kind="comment", text=raw))
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
            current_section = section
            lines.append(_Line(kind="section", text=raw, section=section))
            continue
        if "=" in stripped:
            key, _, value = stripped.partition("=")
            key = key.strip()
            value = value.strip()
            if current_section is None:
                raise AutobootError(
                    f"key {key!r} appears before any [section] header; refusing to parse"
                )
            lines.append(
                _Line(
                    kind="kv",
                    text=raw,
                    section=current_section,
                    key=key,
                    value=value,
                )
            )
            continue
        # Unknown line shape - keep it but flag it as raw so we don't lose data.
        lines.append(_Line(kind="raw", text=raw, section=current_section))

    return Autoboot(lines=lines, trailing_newline=trailing_newline)


def read_autoboot(path: Path) -> Autoboot:
    """Read and parse an autoboot.txt from disk."""
    return parse_autoboot(path.read_text())


def write_autoboot(
    autoboot: Autoboot,
    path: Path,
    mirrors: Iterable[Path] = (),
) -> None:
    """Persist ``autoboot`` to ``path`` and every entry in ``mirrors``.

    Writes are atomic per-file (tempfile + rename). On a mirror failing
    (e.g. boot-B not mounted), the primary write is still committed and the
    exception is re-raised so the caller can log it.
    """
    text = autoboot.to_string()
    _atomic_write_text(path, text)
    for mirror in mirrors:
        _atomic_write_text(mirror, text)


def _atomic_write_text(path: Path, text: str) -> None:
    import os
    import tempfile

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
