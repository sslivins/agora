"""Run the captive portal on the Wi-Fi interface for testing.

Usage (on the Pi):
    sudo systemctl stop agora-player agora-api agora-cms-client
    PYTHONPATH=/opt/agora/src python3 -m provision.test_portal

Then browse to http://<device-ip>:8080 from your phone or computer.
Form submissions are logged but Wi-Fi/AP operations are skipped.
"""

import asyncio
import logging

import uvicorn

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("agora.provision.test_portal")


async def _drain_events():
    """Log portal events as they arrive (instead of acting on them)."""
    from provision.app import portal_events

    while True:
        try:
            event = await asyncio.wait_for(portal_events.get(), timeout=2)
            logger.info("PORTAL EVENT: %s", event)
        except asyncio.TimeoutError:
            continue


async def main():
    from provision.app import app

    config = uvicorn.Config(
        app, host="0.0.0.0", port=8080,
        log_level="info",
    )
    server = uvicorn.Server(config)

    logger.info("Portal test server starting on http://0.0.0.0:8080")
    logger.info("Browse to http://<device-ip>:8080 from your phone")

    await asyncio.gather(
        server.serve(),
        _drain_events(),
    )


if __name__ == "__main__":
    asyncio.run(main())
