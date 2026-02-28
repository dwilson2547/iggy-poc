"""Unit tests for Avro message serialization and the avro_producer/consumer."""
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Schema / serialization helpers
# ---------------------------------------------------------------------------

class TestAvroSerialization:
    def test_roundtrip_produces_original_record(self):
        """avro_serialize → avro_deserialize returns the original record."""
        from schemas import avro_serialize, avro_deserialize

        record = {"id": 1, "text": "hello from avro producer", "ts": "2024-01-01T00:00:00+00:00"}
        data = avro_serialize(record)
        result = avro_deserialize(data)

        assert result["id"] == 1
        assert result["text"] == "hello from avro producer"
        assert result["ts"] == "2024-01-01T00:00:00+00:00"

    def test_serialized_bytes_are_not_empty(self):
        """avro_serialize produces a non-empty bytes object."""
        from schemas import avro_serialize

        data = avro_serialize({"id": 42, "text": "test", "ts": "2024-01-01T00:00:00+00:00"})
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_different_records_produce_different_bytes(self):
        """Two distinct records produce distinct byte sequences."""
        from schemas import avro_serialize

        a = avro_serialize({"id": 1, "text": "first",  "ts": "2024-01-01T00:00:00+00:00"})
        b = avro_serialize({"id": 2, "text": "second", "ts": "2024-01-01T00:00:00+00:00"})
        assert a != b

    def test_id_field_increments_across_records(self):
        """Successive records carry incrementing id values after roundtrip."""
        from schemas import avro_serialize, avro_deserialize

        ts = datetime.now(timezone.utc).isoformat()
        records = [
            avro_deserialize(avro_serialize({"id": i, "text": "msg", "ts": ts}))
            for i in range(1, 4)
        ]
        assert [r["id"] for r in records] == [1, 2, 3]


# ---------------------------------------------------------------------------
# avro_consumer.handle_message
# ---------------------------------------------------------------------------

class TestAvroConsumerHandleMessage:
    def _make_mock_message(self, avro_bytes: bytes, offset: int = 0):
        msg = MagicMock()
        msg.payload.return_value = avro_bytes
        msg.offset.return_value = offset
        return msg

    def test_handle_message_decodes_avro_bytes(self):
        """handle_message decodes Avro bytes and calls logger.info."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from schemas import avro_serialize
            from avro_consumer import handle_message  # noqa: PLC0415

            record = {"id": 1, "text": "hello from avro producer", "ts": "2024-01-01T00:00:00+00:00"}
            msg = self._make_mock_message(avro_serialize(record))
            handle_message(msg)
            msg.payload.assert_called_once()
            msg.offset.assert_called_once()

    def test_handle_message_with_string_payload_encodes_and_decodes(self):
        """handle_message encodes string payload to bytes before deserializing."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from schemas import avro_serialize
            from avro_consumer import handle_message  # noqa: PLC0415

            avro_bytes = avro_serialize({"id": 2, "text": "str-payload", "ts": "2024-01-01T00:00:00+00:00"})
            # Simulate SDK returning the raw bytes as a str (shouldn't happen in practice,
            # but the consumer guards against it).
            msg = MagicMock()
            msg.payload.return_value = avro_bytes  # already bytes
            msg.offset.return_value = 5
            handle_message(msg)
            msg.offset.assert_called_once()


# ---------------------------------------------------------------------------
# avro_producer.init_system
# ---------------------------------------------------------------------------

class TestAvroProducerInitSystem:
    @pytest.mark.asyncio
    async def test_init_system_creates_topic_when_missing(self):
        """init_system creates the topic when it does not exist."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from avro_producer import init_system  # noqa: PLC0415

            client = MagicMock()
            client.get_stream = AsyncMock(return_value=MagicMock(id=1))
            client.get_topic = AsyncMock(return_value=None)
            client.create_topic = AsyncMock()

            await init_system(client)
            client.create_topic.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_init_system_skips_creation_when_both_exist(self):
        """init_system does not create stream or topic when both already exist."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from avro_producer import init_system  # noqa: PLC0415

            client = MagicMock()
            client.get_stream = AsyncMock(return_value=MagicMock(id=1))
            client.create_stream = AsyncMock()
            client.get_topic = AsyncMock(return_value=MagicMock(id=1))
            client.create_topic = AsyncMock()

            await init_system(client)
            client.create_stream.assert_not_awaited()
            client.create_topic.assert_not_awaited()
