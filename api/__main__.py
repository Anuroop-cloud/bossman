"""Run the BOSSman API:  python -m api   (host/port from API_HOST / API_PORT)."""

import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "api.app:app",
        # Default to loopback: this is an operational control plane.
        host=os.getenv("API_HOST_BIND", "127.0.0.1"),
        port=int(os.getenv("API_PORT", "5010")),
        log_level="info",
    )
