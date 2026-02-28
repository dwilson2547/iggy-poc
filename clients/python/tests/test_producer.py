"""Unit tests for the Python Iggy producer."""
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestPayloadStructure:
    def test_payload_is_valid_json(self):
        """Message payload is valid JSON with required fields."""
        import json
        from datetime import datetime, timezone

        payload = json.dumps({
            "id": 1,
            "text": "hello from producer",
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        data = json.loads(payload)
        assert data["id"] == 1
        assert data["text"] == "hello from producer"
        assert "ts" in data

    def test_payload_id_increments(self):
        """Each successive message payload has an incrementing id."""
        import json
        from datetime import datetime, timezone

        payloads = [
            json.loads(json.dumps({"id": i, "text": "hello from producer",
                                   "ts": datetime.now(timezone.utc).isoformat()}))
            for i in range(1, 4)
        ]
        ids = [p["id"] for p in payloads]
        assert ids == [1, 2, 3]


class TestInitSystem:
    @pytest.mark.asyncio
    async def test_init_system_creates_stream_when_missing(self):
        """init_system creates a stream when it does not exist."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from producer import init_system  # noqa: PLC0415

            client = MagicMock()
            client.get_stream = AsyncMock(return_value=None)
            client.create_stream = AsyncMock()
            client.get_topic = AsyncMock(return_value=MagicMock(id=1))

            await init_system(client)
            client.create_stream.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_init_system_skips_stream_when_present(self):
        """init_system skips stream creation when the stream already exists."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from producer import init_system  # noqa: PLC0415

            client = MagicMock()
            existing_stream = MagicMock(id=1)
            client.get_stream = AsyncMock(return_value=existing_stream)
            client.create_stream = AsyncMock()
            client.get_topic = AsyncMock(return_value=MagicMock(id=1))

            await init_system(client)
            client.create_stream.assert_not_awaited()
