/* provision/rgb565.c — fast ARGB32 → RGB565 converter for framebuffer blitting.
 *
 * Compile: gcc -O2 -shared -fPIC -o rgb565.so rgb565.c
 * Usage from Python: ctypes.CDLL("rgb565.so")
 */
#include <stdint.h>
#include <stddef.h>

void argb32_to_rgb565(const uint8_t *src, uint8_t *dst, size_t pixel_count) {
    const uint32_t *in = (const uint32_t *)src;
    uint16_t *out = (uint16_t *)dst;
    for (size_t i = 0; i < pixel_count; i++) {
        uint32_t p = in[i];
        uint8_t r = (p >> 16) & 0xFF;
        uint8_t g = (p >> 8) & 0xFF;
        uint8_t b = p & 0xFF;
        out[i] = ((r >> 3) << 11) | ((g >> 2) << 5) | (b >> 3);
    }
}
