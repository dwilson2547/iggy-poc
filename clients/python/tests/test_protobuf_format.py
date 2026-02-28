"""Unit tests for Protobuf message serialization and the protobuf_producer/consumer."""
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Schema / serialization helpers
# ---------------------------------------------------------------------------

class TestProtobufSerialization:
    def test_roundtrip_produces_original_fields(self):
        """protobuf_serialize → protobuf_deserialize restores all field values."""
        from schemas import protobuf_serialize, protobuf_deserialize

        data = protobuf_serialize(id=1, text="hello from protobuf producer", ts="2024-01-01T00:00:00+00:00")
        event = protobuf_deserialize(data)

        assert event.id == 1
        assert event.text == "hello from protobuf producer"
        assert event.ts == "2024-01-01T00:00:00+00:00"

    def test_serialized_bytes_are_not_empty(self):
        """protobuf_serialize produces a non-empty bytes object."""
        from schemas import protobuf_serialize

        data = protobuf_serialize(id=42, text="test", ts="2024-01-01T00:00:00+00:00")
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_different_ids_produce_different_bytes(self):
        """Two events with different ids produce distinct byte sequences."""
        from schemas import protobuf_serialize

        a = protobuf_serialize(id=1, text="same text", ts="2024-01-01T00:00:00+00:00")
        b = protobuf_serialize(id=2, text="same text", ts="2024-01-01T00:00:00+00:00")
        assert a != b

    def test_id_field_increments_across_events(self):
        """Successive events carry incrementing id values after roundtrip."""
        from schemas import protobuf_serialize, protobuf_deserialize

        ts = datetime.now(timezone.utc).isoformat()
        events = [
            protobuf_deserialize(protobuf_serialize(id=i, text="msg", ts=ts))
            for i in range(1, 4)
        ]
        assert [e.id for e in events] == [1, 2, 3]

    def test_empty_string_fields_roundtrip(self):
        """Protobuf default (empty-string) values survive a roundtrip."""
        from schemas import protobuf_serialize, protobuf_deserialize

        data = protobuf_serialize(id=0, text="", ts="")
        event = protobuf_deserialize(data)
        assert event.id == 0
        assert event.text == ""
        assert event.ts == ""


# ---------------------------------------------------------------------------
# protobuf_consumer.handle_message
# ---------------------------------------------------------------------------

class TestProtobufConsumerHandleMessage:
    def _make_mock_message(self, proto_bytes: bytes, offset: int = 0):
        msg = MagicMock()
        msg.payload.return_value = proto_bytes
        msg.offset.return_value = offset
        return msg

    def test_handle_message_decodes_protobuf_bytes(self):
        """handle_message decodes Protobuf bytes and logs the event fields."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from schemas import protobuf_serialize
            from protobuf_consumer import handle_message  # noqa: PLC0415

            proto_bytes = protobuf_serialize(id=7, text="hello from protobuf producer", ts="2024-01-01T00:00:00+00:00")
            msg = self._make_mock_message(proto_bytes, offset=10)
            handle_message(msg)
            msg.payload.assert_called_once()
            msg.offset.assert_called_once()

    def test_handle_message_uses_offset(self):
        """handle_message includes the message offset in its output."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from schemas import protobuf_serialize
            from protobuf_consumer import handle_message  # noqa: PLC0415

            proto_bytes = protobuf_serialize(id=3, text="ping", ts="2024-01-01T00:00:00+00:00")
            msg = self._make_mock_message(proto_bytes, offset=99)
            handle_message(msg)
            msg.offset.assert_called_once()


# ---------------------------------------------------------------------------
# protobuf_producer.init_system
# ---------------------------------------------------------------------------

class TestProtobufProducerInitSystem:
    @pytest.mark.asyncio
    async def test_init_system_creates_stream_when_missing(self):
        """init_system creates a stream when it does not exist."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from protobuf_producer import init_system  # noqa: PLC0415

            client = MagicMock()
            client.get_stream = AsyncMock(return_value=None)
            client.create_stream = AsyncMock()
            client.get_topic = AsyncMock(return_value=MagicMock(id=1))

            await init_system(client)
            client.create_stream.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_init_system_skips_creation_when_both_exist(self):
        """init_system does not create stream or topic when both already exist."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from protobuf_producer import init_system  # noqa: PLC0415

            client = MagicMock()
            client.get_stream = AsyncMock(return_value=MagicMock(id=1))
            client.create_stream = AsyncMock()
            client.get_topic = AsyncMock(return_value=MagicMock(id=1))
            client.create_topic = AsyncMock()

            await init_system(client)
            client.create_stream.assert_not_awaited()
            client.create_topic.assert_not_awaited()
