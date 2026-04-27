from __future__ import annotations

import logging

import uvicorn

from .api import create_app
from .config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = create_app()


def main() -> None:
    uvicorn.run(
        app,
        host=settings.http_host,
        port=settings.http_port,
        log_level="info",
        loop="uvloop",
        http="httptools",
        access_log=False,
    )


if __name__ == "__main__":
    main()
