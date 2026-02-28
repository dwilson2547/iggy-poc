"""Iggy producer that serializes messages using Protocol Buffers.

The Protobuf schema is defined in ``schemas.py`` (canonical source:
``schemas/event.proto``).  The ``Event`` message class is built at import time
using the ``google.protobuf`` descriptor API so no ``protoc`` code-generation
step is required.

Usage::

    python protobuf_producer.py
"""
import asyncio
from datetime import datetime, timezone

from loguru import logger
from iggy_py import IggyClient, SendMessage

from schemas import protobuf_serialize

STREAM_NAME = "demo-stream"
TOPIC_NAME = "protobuf-topic"
PARTITION_ID = 1
SEND_INTERVAL_SECS = 1.0


async def main() -> None:
    client = IggyClient()
    logger.info("Connecting to Iggy server...")
    await client.connect()
    logger.info("Connected. Logging in...")
    await client.login_user("iggy", "iggy")
    logger.info("Logged in as iggy.")
    await init_system(client)
    await produce_messages(client)


async def init_system(client: IggyClient) -> None:
    """Idempotently create the stream and topic if they don't exist."""
    try:
        stream = await client.get_stream(STREAM_NAME)
        if stream is None:
            await client.create_stream(name=STREAM_NAME)
            logger.info(f"Stream '{STREAM_NAME}' created.")
        else:
            logger.info(f"Stream '{STREAM_NAME}' already exists (id={stream.id}).")
    except Exception as e:
        logger.error(f"Error setting up stream: {e}")
        raise

    try:
        topic = await client.get_topic(STREAM_NAME, TOPIC_NAME)
        if topic is None:
            await client.create_topic(STREAM_NAME, TOPIC_NAME, 1)
            logger.info(f"Topic '{TOPIC_NAME}' created.")
        else:
            logger.info(f"Topic '{TOPIC_NAME}' already exists (id={topic.id}).")
    except Exception as e:
        logger.error(f"Error setting up topic: {e}")
        raise


async def produce_messages(client: IggyClient) -> None:
    logger.info(
        f"Producing Protobuf messages to stream='{STREAM_NAME}' topic='{TOPIC_NAME}' "
        f"partition={PARTITION_ID} every {SEND_INTERVAL_SECS}s. Press Ctrl+C to stop."
    )
    message_id = 0
    while True:
        message_id += 1
        ts = datetime.now(timezone.utc).isoformat()
        payload_bytes = protobuf_serialize(
            id=message_id,
            text="hello from protobuf producer",
            ts=ts,
        )
        message = SendMessage(payload_bytes)
        try:
            await client.send_messages(
                STREAM_NAME,
                TOPIC_NAME,
                PARTITION_ID,
                [message],
            )
            logger.info(
                f"Sent Protobuf message #{message_id}: id={message_id} text='hello from protobuf producer' ts={ts}"
            )
        except Exception as e:
            logger.error(f"Failed to send Protobuf message #{message_id}: {e}")
        await asyncio.sleep(SEND_INTERVAL_SECS)


if __name__ == "__main__":
    asyncio.run(main())
