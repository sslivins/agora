"""Framebuffer display renderer for the provisioning OOBE.

Renders polished setup wizard screens directly to /dev/fb0 using
Cairo + PangoCairo for text/graphics and a C helper for RGB565 conversion.

Usage:
    display = ProvisionDisplay()
    display.show_welcome()
    display.show_connect_phone("Agora-A1B2")
    ...
    display.close()

Threading: all draw methods are synchronous and block for the duration
of the render + blit (~100-200ms on Pi Zero 2 W).  Call from a dedicated
thread when used alongside asyncio.
"""

import ctypes
import json
import logging
import math
import pathlib
import time
from typing import Optional

import cairo
import gi

gi.require_version("Pango", "1.0")
gi.require_version("PangoCairo", "1.0")
from gi.repository import Pango, PangoCairo  # noqa: E402

logger = logging.getLogger("agora.provision.display")

# ── Colour palette ───────────────────────────────────────────────────────────

BLUE = (0.30, 0.65, 1.0)
GREEN = (0.2, 0.9, 0.4)
AMBER = (1.0, 0.8, 0.2)
RED = (1.0, 0.3, 0.3)
WHITE = (1.0, 1.0, 1.0)

# ── Low-level helpers ────────────────────────────────────────────────────────


def _load_rgb565_lib() -> Optional[ctypes.CDLL]:
    """Try to load the C RGB565 converter.  Returns None if unavailable."""
    for path in (
        pathlib.Path(__file__).with_name("rgb565.so"),
        pathlib.Path("/opt/agora/lib/rgb565.so"),
        pathlib.Path("/tmp/rgb565.so"),
    ):
        if path.exists():
            lib = ctypes.CDLL(str(path))
            lib.argb32_to_rgb565.argtypes = [
                ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t,
            ]
            lib.argb32_to_rgb565.restype = None
            logger.info("Loaded RGB565 helper from %s", path)
            return lib
    logger.warning("RGB565 C helper not found — using slow Python fallback")
    return None


def _get_fb_info(fb_path: str = "/dev/fb0") -> tuple[int, int, int]:
    """Read framebuffer dimensions and bits-per-pixel from sysfs."""
    fb_name = pathlib.Path(fb_path).name
    base = pathlib.Path(f"/sys/class/graphics/{fb_name}")
    vsize = (base / "virtual_size").read_text().strip()
    w, h = (int(x) for x in vsize.split(","))
    bpp = int((base / "bits_per_pixel").read_text().strip())
    return w, h, bpp


# ── Drawing primitives ───────────────────────────────────────────────────────


def _draw_bg(ctx: cairo.Context, w: int, h: int) -> None:
    ctx.set_source_rgb(0.06, 0.06, 0.10)
    ctx.paint()


def _draw_text(
    ctx: cairo.Context, cx: float, y: float, text: str, font_desc: str,
    color: tuple = WHITE, alpha: float = 1.0, center: bool = True,
    wrap_width: Optional[float] = None, markup: bool = False,
) -> float:
    """Draw text and return its pixel height."""
    layout = PangoCairo.create_layout(ctx)
    layout.set_font_description(Pango.FontDescription(font_desc))
    if wrap_width:
        layout.set_width(int(wrap_width * Pango.SCALE))
        layout.set_alignment(Pango.Alignment.CENTER)
    if markup:
        layout.set_markup(text, -1)
    else:
        layout.set_text(text, -1)
    _, ext = layout.get_pixel_extents()
    if center and not wrap_width:
        ctx.move_to(cx - ext.width / 2, y)
    elif wrap_width:
        ctx.move_to(cx - wrap_width / 2, y)
    else:
        ctx.move_to(cx, y)
    ctx.set_source_rgba(*color, alpha) if len(color) == 3 else ctx.set_source_rgba(*color)
    PangoCairo.show_layout(ctx, layout)
    return ext.height


def _draw_rounded_rect(
    ctx: cairo.Context, x: float, y: float, w: float, h: float, r: float = 12,
) -> None:
    ctx.new_sub_path()
    ctx.arc(x + w - r, y + r, r, -1.5708, 0)
    ctx.arc(x + w - r, y + h - r, r, 0, 1.5708)
    ctx.arc(x + r, y + h - r, r, 1.5708, 3.14159)
    ctx.arc(x + r, y + r, r, 3.14159, 4.71239)
    ctx.close_path()


def _draw_logo(ctx: cairo.Context, cx: float, y: float) -> float:
    """Draw the AGORA logo and divider.  Returns y below the divider."""
    h = _draw_text(ctx, cx, y, "AGORA", "Sans Bold 64", RED)
    y += h + 30
    ctx.set_source_rgba(1, 1, 1, 0.15)
    ctx.set_line_width(1)
    ctx.move_to(cx - 200, y)
    ctx.line_to(cx + 200, y)
    ctx.stroke()
    return y + 40


def _draw_progress_dots(
    ctx: cairo.Context, cx: float, h: float, current: int, total: int = 5,
) -> None:
    dot_y = h - 80
    total_w = (total - 1) * 30
    start_x = cx - total_w / 2
    for i in range(total):
        ctx.arc(start_x + i * 30, dot_y, 6, 0, 6.28318)
        ctx.set_source_rgb(*BLUE) if i < current else ctx.set_source_rgba(1, 1, 1, 0.2)
        ctx.fill()


def _draw_spinner(
    ctx: cairo.Context, cx: float, cy: float, radius: float, frame: int,
    num_dots: int = 8,
) -> None:
    for i in range(num_dots):
        angle = (2 * math.pi * i / num_dots) + (frame * 2 * math.pi / 20)
        x = cx + radius * math.cos(angle)
        y = cy + radius * math.sin(angle)
        idx = (i + frame) % num_dots
        alpha = 0.1 + 0.9 * (idx / num_dots)
        dot_r = 3 + 3 * (idx / num_dots)
        ctx.arc(x, y, dot_r, 0, 6.28318)
        ctx.set_source_rgba(*BLUE, alpha)
        ctx.fill()


def _draw_checkmark(ctx: cairo.Context, cx: float, cy: float, size: float = 60) -> None:
    """Draw a green circle with a checkmark."""
    ctx.save()
    ctx.new_path()
    ctx.arc(cx, cy, size, 0, 6.28318)
    ctx.set_source_rgba(*GREEN, 0.15)
    ctx.fill_preserve()
    ctx.set_source_rgba(*GREEN, 0.8)
    ctx.set_line_width(3)
    ctx.stroke()
    ctx.set_line_width(max(5, size / 10))
    ctx.set_line_cap(cairo.LINE_CAP_ROUND)
    ctx.set_line_join(cairo.LINE_JOIN_ROUND)
    s = size / 60  # scale factor
    ctx.move_to(cx - 25 * s, cy)
    ctx.line_to(cx - 5 * s, cy + 22 * s)
    ctx.line_to(cx + 30 * s, cy - 22 * s)
    ctx.set_source_rgb(*GREEN)
    ctx.stroke()
    ctx.new_path()
    ctx.restore()


def _draw_x_mark(ctx: cairo.Context, cx: float, cy: float, size: float = 50) -> None:
    """Draw a red circle with an X."""
    ctx.new_path()
    ctx.arc(cx, cy, size, 0, 6.28318)
    ctx.set_source_rgba(*RED, 0.15)
    ctx.fill_preserve()
    ctx.set_source_rgba(*RED, 0.8)
    ctx.set_line_width(3)
    ctx.stroke()
    ctx.set_line_width(max(5, size / 10))
    ctx.set_line_cap(cairo.LINE_CAP_ROUND)
    s = size / 50
    ctx.move_to(cx - 20 * s, cy - 20 * s)
    ctx.line_to(cx + 20 * s, cy + 20 * s)
    ctx.move_to(cx + 20 * s, cy - 20 * s)
    ctx.line_to(cx - 20 * s, cy + 20 * s)
    ctx.set_source_rgb(*RED)
    ctx.stroke()


def _draw_badge(
    ctx: cairo.Context, cx: float, y: float, text: str, font_desc: str,
    bg_color: tuple = BLUE, text_color: tuple = WHITE,
) -> tuple[float, float]:
    """Draw text inside a rounded-rect badge.  Returns (width, height)."""
    layout = PangoCairo.create_layout(ctx)
    layout.set_font_description(Pango.FontDescription(font_desc))
    layout.set_text(text, -1)
    _, ext = layout.get_pixel_extents()
    bw = ext.width + 80
    bh = ext.height + 40
    bx = cx - bw / 2
    _draw_rounded_rect(ctx, bx, y, bw, bh, r=16)
    ctx.set_source_rgba(*bg_color, 0.15)
    ctx.fill_preserve()
    ctx.set_source_rgba(*bg_color, 0.6)
    ctx.set_line_width(2)
    ctx.stroke()
    ctx.move_to(bx + 40, y + 20)
    ctx.set_source_rgb(*text_color)
    PangoCairo.show_layout(ctx, layout)
    return bw, bh


def _draw_qr_code(
    ctx: cairo.Context, cx: float, cy: float, data: str,
    module_size: int = 6, quiet_zone: int = 2,
) -> bool:
    """Draw a QR code centered at (cx, cy).  Returns True if drawn.

    Requires the ``qrcode`` package; gracefully returns False if absent.
    """
    try:
        import qrcode  # type: ignore[import-untyped]
    except ImportError:
        return False

    qr = qrcode.QRCode(
        box_size=1, border=quiet_zone,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
    )
    qr.add_data(data)
    qr.make(fit=True)
    matrix = qr.get_matrix()

    rows = len(matrix)
    cols = len(matrix[0]) if matrix else 0
    total_w = cols * module_size
    total_h = rows * module_size
    x0 = cx - total_w / 2
    y0 = cy - total_h / 2

    # White background with rounded corners
    pad = module_size * 2
    _draw_rounded_rect(
        ctx, x0 - pad, y0 - pad,
        total_w + 2 * pad, total_h + 2 * pad, r=12,
    )
    ctx.set_source_rgb(1, 1, 1)
    ctx.fill()

    # Draw dark modules
    ctx.set_source_rgb(0, 0, 0)
    for r, row in enumerate(matrix):
        for c, cell in enumerate(row):
            if cell:
                ctx.rectangle(
                    x0 + c * module_size, y0 + r * module_size,
                    module_size, module_size,
                )
    ctx.fill()

    return True


# ── ProvisionDisplay ─────────────────────────────────────────────────────────


class ProvisionDisplay:
    """Framebuffer display for the out-of-box provisioning experience."""

    def __init__(self, fb_path: str = "/dev/fb0"):
        self._fb_path = fb_path
        self._rgb565_lib = _load_rgb565_lib()
        try:
            self._width, self._height, self._bpp = _get_fb_info(fb_path)
        except (FileNotFoundError, OSError):
            logger.warning("Framebuffer not available — display disabled")
            self._width = self._height = self._bpp = 0
        self._surface: Optional[cairo.ImageSurface] = None
        self._frame = 0  # animation frame counter
        if self._width:
            self._surface = cairo.ImageSurface(
                cairo.FORMAT_ARGB32, self._width, self._height,
            )
            logger.info(
                "Display ready: %dx%d @ %dbpp", self._width, self._height, self._bpp,
            )

    @property
    def available(self) -> bool:
        return self._surface is not None

    def close(self) -> None:
        """Clear the framebuffer to black and release the surface."""
        if self._surface:
            ctx = cairo.Context(self._surface)
            ctx.set_source_rgb(0, 0, 0)
            ctx.paint()
            self._blit()
        self._surface = None

    # ── Framebuffer output ───────────────────────────────────────────────

    def _blit(self) -> None:
        """Write the current surface to the framebuffer."""
        if not self._surface:
            return
        self._surface.flush()
        data = bytes(self._surface.get_data())
        if self._bpp == 16:
            data = self._convert_rgb565(data)
        with open(self._fb_path, "wb") as fb:
            fb.write(data)

    def _convert_rgb565(self, data: bytes) -> bytes:
        count = self._width * self._height
        if self._rgb565_lib:
            dst = bytearray(count * 2)
            self._rgb565_lib.argb32_to_rgb565(
                ctypes.c_char_p(data),
                (ctypes.c_char * len(dst)).from_buffer(dst),
                count,
            )
            return bytes(dst)
        # Slow Python fallback
        import struct
        pixels = struct.unpack(f"<{count}I", data)
        return struct.pack(
            f"<{count}H",
            *(((p >> 19 & 0x1F) << 11) | ((p >> 10 & 0x3F) << 5) | (p >> 3 & 0x1F)
              for p in pixels),
        )

    def _ctx(self) -> cairo.Context:
        return cairo.Context(self._surface)

    # ── Static screens ───────────────────────────────────────────────────

    def show_welcome(self, frame: int = -1) -> None:
        """Screen: Welcome — 'Setting up your Agora...'

        If *frame* >= 0, draw an animated spinner below the status text.
        """
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.2
        y = _draw_logo(ctx, cx, y) + 40
        _draw_text(ctx, cx, y, "Welcome", "Sans Bold 44", WHITE)
        y += 70
        _draw_text(
            ctx, cx, y,
            "Let's get your device set up.\nThis will only take a minute.",
            "Sans 28", WHITE, alpha=0.7, wrap_width=800,
        )
        y += 120
        _draw_text(ctx, cx, y, "Starting setup...", "Sans 26", AMBER)
        if frame >= 0:
            y += 180
            _draw_spinner(ctx, cx, y, 25, frame)
        _draw_progress_dots(ctx, cx, h, 0)
        self._blit()

    def animate_welcome(self, *, stop_event=None, fps: int = 10) -> None:
        """Animate the welcome screen with a spinner."""
        if not self.available:
            return
        frame = 0
        frame_time = 1.0 / fps
        while True:
            t0 = time.monotonic()
            self.show_welcome(frame=frame)
            frame += 1
            if stop_event and stop_event.is_set():
                break
            elapsed = time.monotonic() - t0
            if elapsed < frame_time:
                time.sleep(frame_time - elapsed)

    def show_connect_phone(self, ssid: str, frame: int = -1) -> None:
        """Screen: Step 1 — 'Connect your phone to Agora-XXXX'.

        If *frame* >= 0, draw an animated spinner next to the amber text.
        """
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.12
        y = _draw_logo(ctx, cx, y) + 10
        _draw_text(ctx, cx, y, "Step 1 of 5", "Sans 24", WHITE, alpha=0.5)
        y += 50
        _draw_text(ctx, cx, y, "Connect Your Phone", "Sans Bold 40", WHITE)
        y += 70
        _, bh = _draw_badge(ctx, cx, y, ssid, "Sans Bold 48")
        y += bh + 40
        th = _draw_text(
            ctx, cx, y,
            "On your phone, open Wi-Fi settings and\n"
            "connect to the network shown above.",
            "Sans 26", WHITE, alpha=0.7, wrap_width=800,
        )
        y += th + 40
        th = _draw_text(ctx, cx, y, "Waiting for connection...", "Sans 28", AMBER)
        if frame >= 0:
            _draw_spinner(ctx, cx, y + th + 65, radius=25, frame=frame, num_dots=8)
        _draw_progress_dots(ctx, cx, h, 1)
        self._blit()

    def animate_connect_phone(
        self, ssid: str, *, stop_event=None, fps: int = 10,
    ) -> None:
        """Animate the connect-phone screen with a spinner."""
        if not self.available:
            return
        frame = 0
        frame_time = 1.0 / fps
        while True:
            t0 = time.monotonic()
            self.show_connect_phone(ssid, frame=frame)
            frame += 1
            if stop_event and stop_event.is_set():
                break
            elapsed = time.monotonic() - t0
            if elapsed < frame_time:
                time.sleep(frame_time - elapsed)

    def show_phone_connected(self) -> None:
        """Screen: Step 2 — 'Phone connected! Open setup page.'"""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.12
        y = _draw_logo(ctx, cx, y) + 10
        th = _draw_text(ctx, cx, y, "Step 2 of 5", "Sans 24", WHITE, alpha=0.5)
        y += th + 20
        th = _draw_text(ctx, cx, y, "Phone Connected!", "Sans Bold 40", GREEN)
        y += th + 30
        _draw_checkmark(ctx, cx, y + 50)
        y += 120  # 50 (offset to center) + 60 (radius) + 10 (gap)
        th = _draw_text(
            ctx, cx, y,
            "A setup page should open on your phone.\n"
            "If it doesn't, open your browser and go to:",
            "Sans 26", WHITE, alpha=0.7, wrap_width=900,
        )
        y += th + 30
        _draw_badge(ctx, cx, y, "http://10.42.0.1", "Monospace Bold 32",
                    bg_color=(0.3, 0.3, 0.4))
        _draw_progress_dots(ctx, cx, h, 2)
        self._blit()

    def show_connecting_wifi(self, network: str) -> None:
        """Screen: Step 3 — 'Connecting to Wi-Fi: NetworkName'."""
        if not self.available:
            return
        self._show_spinner_screen(
            step="Step 3 of 5", title="Connecting to Wi-Fi",
            detail=network, detail_font="Sans Bold 36", detail_color=BLUE,
            subtitle="Please wait while we connect\nto your Wi-Fi network...",
            progress=3,
        )

    def show_wifi_connected(self, network: str) -> None:
        """Screen: Step 3 success — 'Connected to NetworkName'."""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.15
        y = _draw_logo(ctx, cx, y) + 10
        _draw_text(ctx, cx, y, "Step 3 of 5", "Sans 24", WHITE, alpha=0.5)
        y += 50
        _draw_text(ctx, cx, y, "Wi-Fi Connected!", "Sans Bold 40", GREEN)
        y += 70
        _draw_checkmark(ctx, cx, y + 80)
        y += 240
        _draw_text(ctx, cx, y, f"Connected to {network}", "Sans 28", WHITE, alpha=0.7)
        _draw_progress_dots(ctx, cx, h, 3)
        self._blit()

    def show_wifi_failed(self, network: str, error: str = "") -> None:
        """Screen: Step 3 failure — Wi-Fi connection failed."""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.15
        y = _draw_logo(ctx, cx, y) + 10
        _draw_text(ctx, cx, y, "Step 3 of 5", "Sans 24", WHITE, alpha=0.5)
        y += 50
        _draw_text(ctx, cx, y, "Wi-Fi Connection Failed", "Sans Bold 40", RED)
        y += 80
        _draw_x_mark(ctx, cx, y + 45)
        y += 130
        if error:
            _draw_text(ctx, cx, y, error, "Sans 26", WHITE, alpha=0.6)
            y += 50
        th = _draw_text(
            ctx, cx, y,
            f"Could not connect to:",
            "Sans 26", WHITE, alpha=0.5,
        )
        y += th + 10
        th = _draw_text(ctx, cx, y, f"\"{network}\"", "Sans Bold 28", WHITE, alpha=0.7)
        y += th + 100
        _draw_text(
            ctx, cx, y,
            "Restarting setup so you can try again...",
            "Sans 26", WHITE, alpha=0.5,
        )
        _draw_progress_dots(ctx, cx, h, 3)
        self._blit()

    def show_connecting_cms(self, host: str) -> None:
        """Screen: Step 4 — 'Contacting CMS server'."""
        if not self.available:
            return
        self._show_spinner_screen(
            step="Step 4 of 5", title="Contacting Server",
            detail=host, detail_font="Monospace 32", detail_color=BLUE,
            subtitle="Verifying connection to the\ncontent management server...",
            progress=4, y_offset=100,
        )

    def show_cms_connected_pending(self, cms_host: str = "") -> None:
        """Screen: Step 5 — 'Connected to CMS, waiting for adoption'."""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.10
        y = _draw_logo(ctx, cx, y) + 10
        th = _draw_text(ctx, cx, y, "Step 5 of 5", "Sans 24", WHITE, alpha=0.5)
        y += th + 20
        th = _draw_text(ctx, cx, y, "Waiting for Adoption", "Sans Bold 40", AMBER)
        y += th + 30
        th = _draw_text(
            ctx, cx, y,
            "This device has connected to the server\n"
            "and is waiting to be adopted.",
            "Sans 26", WHITE, alpha=0.7, wrap_width=800,
        )
        y += th + 30
        th = _draw_text(
            ctx, cx, y,
            "Open the CMS in your browser and click\n"
            '<b>"Adopt"</b> next to this device:',
            "Sans 26", WHITE, alpha=0.5, wrap_width=800, markup=True,
        )
        y += th + 25
        if cms_host:
            url = f"http://{cms_host}"
            bw, bh = _draw_badge(ctx, cx, y, url, "Monospace Bold 28",
                        bg_color=(0.3, 0.3, 0.4))
            y += bh + 90
        _draw_progress_dots(ctx, cx, h, 5)
        self._blit()

    def show_pairing_qr(
        self,
        *,
        secret: str,
        cms_host: str = "",
        ip: str = "",
        hostname: str = "",
    ) -> None:
        """Screen: show pairing QR for CMS scanner to adopt this device.

        The QR encodes a compact JSON payload ``{"v":1,"secret":<secret>}``
        which the CMS adoption scanner (`cms/static/app.js:_parsePairingQr`)
        accepts directly.  The raw secret is also rendered below the QR as
        a manual-entry fallback.
        """
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)

        y = h * 0.06
        y = _draw_logo(ctx, cx, y) + 10
        th = _draw_text(ctx, cx, y, "Adopt this device", "Sans Bold 38", AMBER)
        y += th + 18
        th = _draw_text(
            ctx, cx, y,
            "Scan with the CMS Adopt button or your phone's camera.",
            "Sans 22", WHITE, alpha=0.7, wrap_width=900,
        )
        y += th + 14

        payload = json.dumps(
            {"v": 1, "secret": secret}, separators=(",", ":"),
        )
        qr_center_y = y + 145
        drawn = _draw_qr_code(ctx, cx, qr_center_y, payload, module_size=5)
        if drawn:
            y = qr_center_y + 165
        else:
            y += 30

        # Manual-entry fallback: print the raw secret in monospace.
        _draw_text(
            ctx, cx, y,
            "Or enter this code manually:",
            "Sans 20", WHITE, alpha=0.5,
        )
        y += 30
        bw, bh = _draw_badge(
            ctx, cx, y, secret, "Monospace Bold 26",
            bg_color=(0.3, 0.3, 0.4),
        )
        y += bh + 18

        # LAN context: IP / hostname / cms_host badges (compact row).
        for label, value, font in (
            ("IP", ip, "Monospace Bold 22"),
            ("mDNS", hostname, "Monospace Bold 22"),
            ("CMS", cms_host, "Monospace Bold 22"),
        ):
            if not value:
                continue
            th = _draw_text(
                ctx, cx, y, f"{label}: {value}", font, WHITE, alpha=0.55,
            )
            y += th + 4

        _draw_progress_dots(ctx, cx, h, 5)
        self._blit()

    def show_adopted(self) -> None:
        """Screen: Setup complete — device was adopted."""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.18
        y = _draw_logo(ctx, cx, y) + 40
        _draw_checkmark(ctx, cx, y + 60, size=60)
        y += 155
        th = _draw_text(ctx, cx, y, "Setup Complete!", "Sans Bold 44", GREEN)
        y += th + 30
        th = _draw_text(
            ctx, cx, y,
            "Your device has been adopted by the server.",
            "Sans 28", WHITE, alpha=0.7,
        )
        y += th + 30
        th = _draw_text(
            ctx, cx, y,
            "Content will appear shortly.",
            "Sans 28", WHITE, alpha=0.7,
        )
        _draw_progress_dots(ctx, cx, h, 5)
        self._blit()

    def show_cms_failed(self, host: str, error: str = "") -> None:
        """Screen: CMS connection failed."""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.15
        y = _draw_logo(ctx, cx, y) + 40
        _draw_x_mark(ctx, cx, y + 45)
        y += 130
        _draw_text(ctx, cx, y, "Server Connection Failed", "Sans Bold 40", RED)
        y += 65
        _draw_text(ctx, cx, y, host, "Monospace 26", WHITE, alpha=0.6)
        y += 50
        if error:
            _draw_text(ctx, cx, y, error, "Sans 24", WHITE, alpha=0.5)
            y += 45
        _draw_text(
            ctx, cx, y,
            "Check that the CMS server is running\n"
            "and reachable from this network.\n\n"
            "The device will retry automatically.",
            "Sans 26", WHITE, alpha=0.5, wrap_width=800,
        )
        _draw_progress_dots(ctx, cx, h, 4)
        self._blit()

    def show_cms_reconfigure(self, url: str) -> None:
        """Screen: CMS connection failed — QR code to reconfigure."""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)

        y = h * 0.07
        y = _draw_logo(ctx, cx, y) + 10
        _draw_text(ctx, cx, y, "Server Connection Failed", "Sans Bold 38", RED)
        y += 55
        _draw_text(
            ctx, cx, y,
            "Scan the QR code with your phone to\n"
            "update the server address.",
            "Sans 26", WHITE, alpha=0.7, wrap_width=800,
        )
        y += 85

        # QR code (or URL fallback if qrcode package unavailable)
        qr_center_y = y + 130
        drawn = _draw_qr_code(ctx, cx, qr_center_y, url, module_size=5)
        if drawn:
            y = qr_center_y + 150
        else:
            y += 30

        _draw_text(
            ctx, cx, y,
            "Or open this address in your browser:",
            "Sans 24", WHITE, alpha=0.5,
        )
        y += 45
        _draw_badge(
            ctx, cx, y, url, "Monospace Bold 28",
            bg_color=(0.3, 0.3, 0.4),
        )
        _draw_progress_dots(ctx, cx, h, 4)
        self._blit()

    # ── Ethernet / no-network screens ──────────────────────────────────

    def show_ethernet_connected(self, ip: str, hostname: str = "") -> None:
        """Screen: Ethernet connected — show IP and mDNS name, waiting for CMS."""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.12
        y = _draw_logo(ctx, cx, y) + 30
        th = _draw_text(ctx, cx, y, "Ethernet Connected", "Sans Bold 40", GREEN)
        y += th + 40
        th = _draw_text(
            ctx, cx, y,
            "This device is connected via Ethernet\n"
            "and is ready to be adopted.",
            "Sans 26", WHITE, alpha=0.7, wrap_width=800,
        )
        y += th + 40
        bw, bh = _draw_badge(
            ctx, cx, y, ip, "Monospace Bold 32",
            bg_color=(0.3, 0.3, 0.4),
        )
        y += bh + 20
        if hostname:
            bw, bh = _draw_badge(
                ctx, cx, y, hostname, "Monospace Bold 28",
                bg_color=(0.25, 0.25, 0.35),
            )
            y += bh + 30
        th = _draw_text(
            ctx, cx, y,
            "Open the CMS and adopt this device to begin.",
            "Sans 24", WHITE, alpha=0.5, wrap_width=800,
        )
        self._blit()

    def show_no_network(self) -> None:
        """Screen: No network interfaces available — prompt user to connect ethernet."""
        if not self.available:
            return
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.15
        y = _draw_logo(ctx, cx, y) + 40
        th = _draw_text(ctx, cx, y, "No Network Connection", "Sans Bold 40", RED)
        y += th + 40
        th = _draw_text(
            ctx, cx, y,
            "This device has no WiFi adapter and\n"
            "no Ethernet cable connected.",
            "Sans 28", WHITE, alpha=0.7, wrap_width=800,
        )
        y += th + 50
        th = _draw_text(
            ctx, cx, y,
            "Please connect an Ethernet cable\n"
            "and reboot the device.",
            "Sans 28", AMBER, wrap_width=800,
        )
        self._blit()

    # ── Animated spinner screen (reusable) ───────────────────────────────

    def _show_spinner_screen(
        self, *, step: str, title: str, detail: str, detail_font: str,
        detail_color: tuple, subtitle: str, progress: int, y_offset: int = 0,
    ) -> None:
        """Render a single frame of a spinner screen and blit it."""
        ctx = self._ctx()
        w, h = self._width, self._height
        cx = w / 2
        _draw_bg(ctx, w, h)
        y = h * 0.12
        y = _draw_logo(ctx, cx, y) + 10
        _draw_text(ctx, cx, y, step, "Sans 24", WHITE, alpha=0.5)
        y += 50 + y_offset
        _draw_text(ctx, cx, y, title, "Sans Bold 40", WHITE)
        y += 80
        _draw_text(ctx, cx, y, detail, detail_font, detail_color)
        y += 70
        _draw_spinner(ctx, cx, y + 30, 20, self._frame)
        y += 85
        _draw_text(ctx, cx, y, subtitle, "Sans 26", WHITE, alpha=0.6, wrap_width=800)
        _draw_progress_dots(ctx, cx, h, progress)
        self._frame += 1
        self._blit()

    def animate_spinner(
        self, *, step: str, title: str, detail: str, detail_font: str = "Sans Bold 36",
        detail_color: tuple = BLUE, subtitle: str, progress: int,
        duration: float = 0.0, stop_event=None, fps: int = 10, y_offset: int = 0,
    ) -> None:
        """Run a spinner animation loop.

        Runs for *duration* seconds, or until *stop_event* is set (threading.Event
        or asyncio-compatible), whichever comes first.  If both are zero/None,
        renders a single frame.
        """
        if not self.available:
            return
        frame_time = 1.0 / fps
        deadline = time.monotonic() + duration if duration > 0 else 0

        while True:
            t0 = time.monotonic()
            self._show_spinner_screen(
                step=step, title=title, detail=detail, detail_font=detail_font,
                detail_color=detail_color, subtitle=subtitle, progress=progress,
                y_offset=y_offset,
            )
            # Check exit conditions
            if stop_event and stop_event.is_set():
                break
            if deadline and time.monotonic() >= deadline:
                break
            if not deadline and not stop_event:
                break  # single frame
            # Frame pacing
            elapsed = time.monotonic() - t0
            if elapsed < frame_time:
                time.sleep(frame_time - elapsed)
