"""Entry point for the provisioning service."""

import uvicorn

from provision.app import app  # noqa: F401

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80, log_level="info")
