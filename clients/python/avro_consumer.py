"""Iggy consumer that deserializes Apache Avro messages.

The Avro schema is defined in ``schemas.py`` (canonical source:
``schemas/event.avsc``).  Each message payload is treated as raw schemaless
Avro bytes and decoded back to a Python ``dict``.

Usage::

    python avro_consumer.py
"""
import asyncio

from loguru import logger
from iggy_py import IggyClient, ReceiveMessage, PollingStrategy

from schemas import avro_deserialize

STREAM_NAME = "demo-stream"
TOPIC_NAME = "avro-topic"
PARTITION_ID = 1
POLL_INTERVAL_SECS = 0.5
MESSAGES_PER_BATCH = 10


async def main() -> None:
    client = IggyClient()
    logger.info("Connecting to Iggy server...")
    await client.connect()
    logger.info("Connected. Logging in...")
    await client.login_user("iggy", "iggy")
    logger.info("Logged in as iggy.")
    await consume_messages(client)


async def consume_messages(client: IggyClient) -> None:
    logger.info(
        f"Consuming Avro messages from stream='{STREAM_NAME}' topic='{TOPIC_NAME}' "
        f"partition={PARTITION_ID}. Press Ctrl+C to stop."
    )
    while True:
        try:
            polled = await client.poll_messages(
                STREAM_NAME,
                TOPIC_NAME,
                PARTITION_ID,
                PollingStrategy.Next(),
                MESSAGES_PER_BATCH,
                auto_commit=True,
            )
            if not polled:
                logger.debug("No new messages — waiting...")
                await asyncio.sleep(POLL_INTERVAL_SECS)
                continue

            for msg in polled:
                handle_message(msg)

            await asyncio.sleep(POLL_INTERVAL_SECS)
        except asyncio.CancelledError:
            logger.info("Consumer cancelled.")
            break
        except Exception:
            logger.exception("Error while consuming — retrying...")
            await asyncio.sleep(POLL_INTERVAL_SECS)


def handle_message(message: ReceiveMessage) -> None:
    payload = message.payload()
    if not isinstance(payload, bytes):
        payload = payload.encode("utf-8")
    record = avro_deserialize(payload)
    logger.info(f"[offset={message.offset()}] {record}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down Avro consumer.")
