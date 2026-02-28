"""Iggy consumer that deserializes Protocol Buffers messages.

The Protobuf schema is defined in ``schemas.py`` (canonical source:
``schemas/event.proto``).  Each message payload is treated as Protobuf
wire-format bytes and decoded back to an ``Event`` message object.

Usage::

    python protobuf_consumer.py
"""
import asyncio

from loguru import logger
from iggy_py import IggyClient, ReceiveMessage, PollingStrategy

from schemas import protobuf_deserialize

STREAM_NAME = "demo-stream"
TOPIC_NAME = "protobuf-topic"
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
        f"Consuming Protobuf messages from stream='{STREAM_NAME}' topic='{TOPIC_NAME}' "
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
    event = protobuf_deserialize(payload)
    logger.info(
        f"[offset={message.offset()}] id={event.id} text='{event.text}' ts='{event.ts}'"
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down Protobuf consumer.")
