"""Unit tests for the Python Iggy consumer."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add parent directory to path so we can import consumer without iggy_py installed
sys.path.insert(0, str(Path(__file__).parent.parent))


def _make_mock_message(payload, offset=0):
    """Helper to create a mock ReceiveMessage."""
    msg = MagicMock()
    msg.payload.return_value = payload
    msg.offset.return_value = offset
    return msg


class TestHandleMessage:
    def test_handle_bytes_payload(self):
        """handle_message decodes byte payloads to UTF-8."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from consumer import handle_message  # noqa: PLC0415

            msg = _make_mock_message(b'{"id": 1, "text": "hello"}', offset=0)
            # Should not raise; side effect is a logger.info call
            handle_message(msg)
            msg.payload.assert_called_once()
            msg.offset.assert_called_once()

    def test_handle_string_payload(self):
        """handle_message passes through string payloads unchanged."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from consumer import handle_message  # noqa: PLC0415

            msg = _make_mock_message('{"id": 2, "text": "world"}', offset=1)
            handle_message(msg)
            msg.payload.assert_called_once()

    def test_handle_message_offset_used(self):
        """handle_message uses the message offset in its log output."""
        with patch.dict("sys.modules", {"iggy_py": MagicMock(), "loguru": MagicMock()}):
            from consumer import handle_message  # noqa: PLC0415

            msg = _make_mock_message(b"ping", offset=42)
            handle_message(msg)
            msg.offset.assert_called_once()
