#!/usr/bin/env python3
"""Generate spinner frame PNGs for the Plymouth boot splash.

Produces 12 frames of 8 dots arranged in a circle. Each frame highlights
one dot brightly and fades the trailing dots, creating a rotating spinner
effect. Output: spinner-00.png through spinner-11.png in the same directory.
"""

import math
import struct
import zlib
import os

FRAME_COUNT = 12
DOT_COUNT = 8
IMG_SIZE = 64          # 64x64 px canvas
DOT_RADIUS = 5
CIRCLE_RADIUS = 24     # dots sit on this radius from center
CENTER = IMG_SIZE // 2

# Brightness per dot relative to the "leading" dot (index 0 = brightest)
# 8 dots, trail fades out
ALPHA_MAP = [255, 200, 150, 110, 80, 55, 40, 30]


def make_png(width, height, rgba_pixels):
    """Create a minimal PNG file from raw RGBA pixel data."""
    def _chunk(chunk_type, data):
        c = chunk_type + data
        crc = struct.pack('>I', zlib.crc32(c) & 0xFFFFFFFF)
        return struct.pack('>I', len(data)) + c + crc

    header = b'\x89PNG\r\n\x1a\n'
    ihdr = _chunk(b'IHDR', struct.pack('>IIBBBBB', width, height, 8, 6, 0, 0, 0))

    raw = b''
    for y in range(height):
        raw += b'\x00'  # filter byte
        raw += rgba_pixels[y * width * 4:(y + 1) * width * 4]

    idat = _chunk(b'IDAT', zlib.compress(raw, 9))
    iend = _chunk(b'IEND', b'')
    return header + ihdr + idat + iend


def draw_filled_circle(buf, width, cx, cy, r, color_rgba):
    """Draw a filled anti-aliased circle into an RGBA buffer."""
    r2 = r * r
    for dy in range(-r - 1, r + 2):
        for dx in range(-r - 1, r + 2):
            px, py = cx + dx, cy + dy
            if 0 <= px < width and 0 <= py < width:
                dist2 = dx * dx + dy * dy
                if dist2 <= r2:
                    alpha = color_rgba[3]
                elif dist2 <= (r + 1) ** 2:
                    # AA fringe
                    frac = 1.0 - (math.sqrt(dist2) - r)
                    alpha = int(color_rgba[3] * max(0, frac))
                else:
                    continue
                idx = (py * width + px) * 4
                # Simple alpha composite over transparent background
                buf[idx] = color_rgba[0]
                buf[idx + 1] = color_rgba[1]
                buf[idx + 2] = color_rgba[2]
                buf[idx + 3] = max(buf[idx + 3], alpha)


def generate_frames(out_dir):
    angle_step = 2 * math.pi / DOT_COUNT
    frame_step = DOT_COUNT / FRAME_COUNT  # fractional dot advance per frame

    for frame in range(FRAME_COUNT):
        buf = bytearray(IMG_SIZE * IMG_SIZE * 4)  # RGBA, all transparent

        lead_index = (frame * frame_step) % DOT_COUNT

        for dot in range(DOT_COUNT):
            angle = dot * angle_step - math.pi / 2  # start at top
            dx = int(round(CENTER + CIRCLE_RADIUS * math.cos(angle)))
            dy = int(round(CENTER + CIRCLE_RADIUS * math.sin(angle)))

            # How far behind the lead is this dot?
            offset = (dot - lead_index) % DOT_COUNT
            trail = int(offset)
            alpha = ALPHA_MAP[min(trail, len(ALPHA_MAP) - 1)]

            draw_filled_circle(buf, IMG_SIZE, dx, dy, DOT_RADIUS, (255, 255, 255, alpha))

        png_data = make_png(IMG_SIZE, IMG_SIZE, bytes(buf))
        path = os.path.join(out_dir, f'spinner-{frame:02d}.png')
        with open(path, 'wb') as f:
            f.write(png_data)
        print(f'  {path}')


if __name__ == '__main__':
    out = os.path.dirname(os.path.abspath(__file__))
    print(f'Generating {FRAME_COUNT} spinner frames in {out}/')
    generate_frames(out)
    print('Done.')
