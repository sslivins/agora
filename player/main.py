"""Entry point for the Agora Player service.

See ``docs/image-catalog.md`` for the release-driven base-image catalog
publishing pipeline (consumed by agora-cms's Imager feature).
"""

import logging
import os
import sys

# Ensure the project root is on the path so shared/ is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from player.service import AgoraPlayer  # noqa: E402


def main():
    log_level = os.environ.get("AGORA_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    base_path = os.environ.get("AGORA_BASE", "/opt/agora")
    player = AgoraPlayer(base_path=base_path)
    player.run()


if __name__ == "__main__":
    main()
