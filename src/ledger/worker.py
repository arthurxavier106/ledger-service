"""Processo do worker de outbox.

Roda separado da API de proposito: entrega de webhook e I/O lento e sujeito a
endpoint de terceiro fora do ar. Se isso vivesse dentro do request da API, uma
queda do cliente viraria latencia no write path do ledger.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from ledger.db import SessionFactory, engine
from ledger.outbox import run_worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("ledger.worker")


async def main() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    logger.info("outbox worker iniciado")
    try:
        await run_worker(SessionFactory, stop=stop)
    finally:
        await engine.dispose()
        logger.info("outbox worker encerrado")


if __name__ == "__main__":
    asyncio.run(main())
