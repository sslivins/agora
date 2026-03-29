"""CMS client entry point — run as a standalone service."""

import asyncio
import logging
import sys

from api.config import load_settings
from cms_client.service import CMSClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("agora.cms_client")


async def main() -> None:
    settings = load_settings()
    settings.ensure_dirs()

    client = CMSClient(settings)

    try:
        await client.run()
    except KeyboardInterrupt:
        pass
    finally:
        await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
