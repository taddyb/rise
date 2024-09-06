import asyncio

import aio_pika

from src.rise.app.api.services.rise import RISE
from src.rise.app.core.cache import get_settings
from src.rise.app.core.logging_module import setup_logger
from src.rise.app.core.settings import Settings

# import uvicorn
# from fastapi import FastAPI, status
# from fastapi.responses import Response


PARALLEL_TASKS = 10

log = setup_logger("default", "consumer.log")

# app = FastAPI(title="Consumer")

# @app.head("/health")
# async def health_check():
#     return Response(status_code=status.HTTP_200_OK)


async def main(settings: Settings) -> None:
    connection = await aio_pika.connect_robust(settings.aio_pika_url)
    rise = RISE()

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=PARALLEL_TASKS)
        priority_queue = await channel.declare_queue(
            settings.priority_queue,
            durable=True,
        )
        base_queue = await channel.declare_queue(
            settings.base_queue,
            durable=True,
        )
        error_queue = await channel.declare_queue(
            settings.error_queue,
            durable=True,
        )

        log.info("Consumer started")

        # try:
        await priority_queue.consume(rise.process_flood_request)
        await base_queue.consume(rise.process_request)
        await error_queue.consume(rise.process_error)

        try:
            await asyncio.Future()
        finally:
            await connection.close()


if __name__ == "__main__":
    log.info("Starting Consumer")
    asyncio.run(main(get_settings()))
    # uvicorn.run(app, host="0.0.0.0", port=8080)
