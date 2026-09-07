"""Unit tests: protocol envelope parse/build."""

import pytest

from app.websocket.protocol import Envelope, MessageType, ProtocolError, parse_envelope


def test_envelope_roundtrip():
    envelope = Envelope(type=MessageType.HEARTBEAT, data={"seq": 1})
    raw = envelope.to_json()
    parsed = parse_envelope(raw)
    assert parsed.id == envelope.id
    assert parsed.type == "heartbeat"
    assert parsed.version == 1
    assert parsed.data == {"seq": 1}


def test_parse_missing_fields():
    with pytest.raises(ProtocolError):
        parse_envelope('{"id": "x"}')
    with pytest.raises(ProtocolError):
        parse_envelope('{"type": "heartbeat"}')


def test_parse_invalid_json():
    with pytest.raises(ProtocolError):
        parse_envelope("not json")
    with pytest.raises(ProtocolError):
        parse_envelope("[1,2,3]")


def test_parse_data_must_be_object():
    with pytest.raises(ProtocolError):
        parse_envelope('{"id":"a","type":"message","version":1,"data":[1]}')


def test_message_type_values():
    assert MessageType.HEARTBEAT == "heartbeat"
    assert MessageType.DEVICE_CONNECTED == "device.connected"
    assert MessageType.MESSAGE_ACK == "message_ack"
