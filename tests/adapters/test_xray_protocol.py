"""Unit tests for the xrayProtocol low-level client.

All tests exercise encoding/decoding logic without requiring a live server.
Binary payloads are constructed by hand to verify the decoder.
"""

from __future__ import annotations

import math
import struct

import pytest

from tools.xraybench.adapters.xray_protocol import (
    COL_BOOL,
    COL_DOUBLE,
    COL_INT64,
    COL_NULL,
    COL_STRING,
    FRAME_HEADER_SIZE,
    LANG_CYPHER,
    LANG_GFQL,
    MSG_BATCH,
    MSG_COMPLETE,
    MSG_ERROR,
    MSG_EXECUTE,
    MSG_HELLO,
    MSG_HELLO_OK,
    MSG_SCHEMA,
    OPT_EXPLAIN,
    OPT_PROFILE,
    OPT_READ_ONLY,
    XrayProtocolClient,
    _apply_null_bitmap,
    encode_execute_payload,
    encode_frame,
    encode_hello_payload,
)


# ---------------------------------------------------------------------------
# HELLO encoding
# ---------------------------------------------------------------------------


class TestEncodeHello:
    def test_encode_hello_basic(self) -> None:
        """Verify HELLO frame binary format with empty credentials."""
        payload = encode_hello_payload()
        # u16 protocol_version=1, u16 caps=0, u32 auth_len=1 (just ":")
        assert len(payload) == 2 + 2 + 4 + 1  # ":" is 1 byte
        version, caps, auth_len = struct.unpack_from("<HHI", payload, 0)
        assert version == 1
        assert caps == 0
        assert auth_len == 1
        assert payload[8:] == b":"

    def test_encode_hello_with_credentials(self) -> None:
        """Verify HELLO payload with username:password."""
        payload = encode_hello_payload(
            username="admin", password="secret", capabilities=0x05
        )
        version, caps, auth_len = struct.unpack_from("<HHI", payload, 0)
        assert version == 1
        assert caps == 0x05
        auth_token = payload[8 : 8 + auth_len]
        assert auth_token == b"admin:secret"

    def test_encode_hello_frame(self) -> None:
        """Verify full HELLO frame (header + payload)."""
        payload = encode_hello_payload(username="u", password="p")
        frame = encode_frame(MSG_HELLO, 0, payload)
        # Header is 8 bytes
        assert len(frame) == FRAME_HEADER_SIZE + len(payload)
        plen, msg_type, flags, qid = struct.unpack_from("<IBBH", frame, 0)
        assert plen == len(payload)
        assert msg_type == MSG_HELLO
        assert flags == 0
        assert qid == 0


# ---------------------------------------------------------------------------
# EXECUTE encoding
# ---------------------------------------------------------------------------


class TestEncodeExecute:
    def test_encode_execute_cypher(self) -> None:
        """Verify EXECUTE frame binary format for a Cypher query."""
        query = "RETURN 1 AS x"
        payload = encode_execute_payload(query, language=LANG_CYPHER)
        offset = 0

        # u8 language
        assert payload[offset] == LANG_CYPHER
        offset += 1

        # u32 query_length
        (qlen,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        assert qlen == len(query.encode("utf-8"))

        # N query bytes
        qbytes = payload[offset : offset + qlen]
        offset += qlen
        assert qbytes == query.encode("utf-8")

        # u32 parameter_count
        (param_count,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        assert param_count == 0

        # u32 options
        (options,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        assert options == 0

        # u16 projection_count
        (proj_count,) = struct.unpack_from("<H", payload, offset)
        offset += 2
        assert proj_count == 0

    def test_encode_execute_gfql_with_options(self) -> None:
        """Verify EXECUTE with GFQL language and PROFILE+EXPLAIN options."""
        query = "GFQL: some query"
        options = OPT_PROFILE | OPT_EXPLAIN
        payload = encode_execute_payload(
            query, language=LANG_GFQL, options=options
        )
        assert payload[0] == LANG_GFQL

        # Skip to options field
        offset = 1 + 4 + len(query.encode("utf-8")) + 4
        (opt,) = struct.unpack_from("<I", payload, offset)
        assert opt == options

    def test_encode_execute_frame(self) -> None:
        """Verify full EXECUTE frame wrapping."""
        payload = encode_execute_payload("MATCH (n) RETURN n")
        frame = encode_frame(MSG_EXECUTE, 42, payload)
        plen, msg_type, flags, qid = struct.unpack_from("<IBBH", frame, 0)
        assert msg_type == MSG_EXECUTE
        assert qid == 42
        assert plen == len(payload)


# ---------------------------------------------------------------------------
# SCHEMA decoding
# ---------------------------------------------------------------------------


class TestDecodeSchema:
    def test_decode_schema_single_column(self) -> None:
        """Decode a hand-crafted SCHEMA payload with one INT64 column."""
        client = XrayProtocolClient("localhost")
        # u16 column_count=1
        payload = struct.pack("<H", 1)
        # column: u8 type=INT64, u16 name_length=2, "id"
        payload += struct.pack("<B", COL_INT64)
        payload += struct.pack("<H", 2)
        payload += b"id"

        columns = client._decode_schema(payload)
        assert len(columns) == 1
        assert columns[0] == ("id", COL_INT64)

    def test_decode_schema_multiple_columns(self) -> None:
        """Decode SCHEMA with three columns of different types."""
        client = XrayProtocolClient("localhost")
        payload = struct.pack("<H", 3)

        # Column 1: INT64 "id"
        payload += struct.pack("<B", COL_INT64)
        payload += struct.pack("<H", 2)
        payload += b"id"

        # Column 2: STRING "name"
        payload += struct.pack("<B", COL_STRING)
        payload += struct.pack("<H", 4)
        payload += b"name"

        # Column 3: BOOL "active"
        payload += struct.pack("<B", COL_BOOL)
        payload += struct.pack("<H", 6)
        payload += b"active"

        columns = client._decode_schema(payload)
        assert len(columns) == 3
        assert columns[0] == ("id", COL_INT64)
        assert columns[1] == ("name", COL_STRING)
        assert columns[2] == ("active", COL_BOOL)


# ---------------------------------------------------------------------------
# BATCH decoding — INT64
# ---------------------------------------------------------------------------


def _build_int64_batch(values: list[int], col_name: str = "x") -> tuple[bytes, list[tuple[str, int]]]:
    """Build a BATCH payload with one INT64 column.

    Returns (payload, schema).
    """
    row_count = len(values)
    col_count = 1
    sv_len = 0

    # Header: u32 row_count, u16 col_count, u32 sv_length
    batch = struct.pack("<IHI", row_count, col_count, sv_len)

    # Column data
    data = struct.pack(f"<{row_count}q", *values)
    data_length = len(data)

    batch += struct.pack("<I", data_length)

    # Padding to align to 8 bytes — compute alignment for offset after data_length field
    # Current offset in batch: 4 + 2 + 4 + 4 = 14
    current_offset = len(batch)
    padding = (8 - (current_offset % 8)) % 8
    batch += b"\x00" * padding

    batch += data

    # Null bitmap: all valid
    bitmap_len = math.ceil(row_count / 8)
    bitmap = bytearray(bitmap_len)
    for i in range(row_count):
        bitmap[i // 8] |= 1 << (i % 8)
    batch += bytes(bitmap)

    schema = [(col_name, COL_INT64)]
    return (batch, schema)


class TestDecodeInt64Batch:
    def test_decode_int64_3_rows(self) -> None:
        """Decode a BATCH with one INT64 column, 3 rows: [42, 0, -1]."""
        payload, schema = _build_int64_batch([42, 0, -1])
        client = XrayProtocolClient("localhost")
        rows = client._decode_batch(payload, schema)

        assert len(rows) == 3
        assert rows[0]["x"] == 42
        assert rows[1]["x"] == 0
        assert rows[2]["x"] == -1

    def test_decode_int64_single_row(self) -> None:
        """Decode a BATCH with one INT64 column, 1 row."""
        payload, schema = _build_int64_batch([999])
        client = XrayProtocolClient("localhost")
        rows = client._decode_batch(payload, schema)

        assert len(rows) == 1
        assert rows[0]["x"] == 999

    def test_decode_int64_large_values(self) -> None:
        """Decode INT64 column with large and negative values."""
        vals = [2**62, -(2**62), 0, 2**63 - 1]
        payload, schema = _build_int64_batch(vals)
        client = XrayProtocolClient("localhost")
        rows = client._decode_batch(payload, schema)

        assert len(rows) == 4
        for i, v in enumerate(vals):
            assert rows[i]["x"] == v


# ---------------------------------------------------------------------------
# BATCH decoding — STRING
# ---------------------------------------------------------------------------


def _build_string_batch(
    values: list[str], col_name: str = "s"
) -> tuple[bytes, list[tuple[str, int]]]:
    """Build a BATCH payload with one STRING column.

    STRING column layout:
        u32 total_string_bytes
        u32[row_count+1] offsets
        bytes[total_string_bytes] string_data
    """
    row_count = len(values)
    col_count = 1
    sv_len = 0

    batch = struct.pack("<IHI", row_count, col_count, sv_len)

    # Build string column data
    encoded = [v.encode("utf-8") for v in values]
    total_string_bytes = sum(len(e) for e in encoded)

    col_data = struct.pack("<I", total_string_bytes)
    # Offsets
    offset = 0
    offsets = [0]
    for e in encoded:
        offset += len(e)
        offsets.append(offset)
    for o in offsets:
        col_data += struct.pack("<I", o)
    # String data blob
    for e in encoded:
        col_data += e

    data_length = len(col_data)
    batch += struct.pack("<I", data_length)

    # Padding
    current_offset = len(batch)
    padding = (8 - (current_offset % 8)) % 8
    batch += b"\x00" * padding

    batch += col_data

    # Null bitmap: all valid
    bitmap_len = math.ceil(row_count / 8)
    bitmap = bytearray(bitmap_len)
    for i in range(row_count):
        bitmap[i // 8] |= 1 << (i % 8)
    batch += bytes(bitmap)

    schema = [(col_name, COL_STRING)]
    return (batch, schema)


class TestDecodeStringBatch:
    def test_decode_string_2_rows(self) -> None:
        """Decode a BATCH with one STRING column, 2 rows."""
        payload, schema = _build_string_batch(["hello", "world"])
        client = XrayProtocolClient("localhost")
        rows = client._decode_batch(payload, schema)

        assert len(rows) == 2
        assert rows[0]["s"] == "hello"
        assert rows[1]["s"] == "world"

    def test_decode_string_empty(self) -> None:
        """Decode STRING column with empty strings."""
        payload, schema = _build_string_batch(["", "a", ""])
        client = XrayProtocolClient("localhost")
        rows = client._decode_batch(payload, schema)

        assert len(rows) == 3
        assert rows[0]["s"] == ""
        assert rows[1]["s"] == "a"
        assert rows[2]["s"] == ""

    def test_decode_string_unicode(self) -> None:
        """Decode STRING column with UTF-8 characters."""
        payload, schema = _build_string_batch(["cafe\u0301", "naiv\u0308e"])
        client = XrayProtocolClient("localhost")
        rows = client._decode_batch(payload, schema)

        assert len(rows) == 2
        assert rows[0]["s"] == "cafe\u0301"
        assert rows[1]["s"] == "naiv\u0308e"


# ---------------------------------------------------------------------------
# BATCH decoding — BOOL
# ---------------------------------------------------------------------------


def _build_bool_batch(
    values: list[bool], col_name: str = "b"
) -> tuple[bytes, list[tuple[str, int]]]:
    """Build a BATCH payload with one BOOL column (1 byte per row)."""
    row_count = len(values)
    col_count = 1
    sv_len = 0

    batch = struct.pack("<IHI", row_count, col_count, sv_len)

    data = bytes([1 if v else 0 for v in values])
    data_length = len(data)

    batch += struct.pack("<I", data_length)

    current_offset = len(batch)
    padding = (8 - (current_offset % 8)) % 8
    batch += b"\x00" * padding

    batch += data

    # Null bitmap: all valid
    bitmap_len = math.ceil(row_count / 8)
    bitmap = bytearray(bitmap_len)
    for i in range(row_count):
        bitmap[i // 8] |= 1 << (i % 8)
    batch += bytes(bitmap)

    schema = [(col_name, COL_BOOL)]
    return (batch, schema)


class TestDecodeBoolBatch:
    def test_decode_bool_column(self) -> None:
        """Decode a BATCH with one BOOL column."""
        payload, schema = _build_bool_batch([True, False, True, True])
        client = XrayProtocolClient("localhost")
        rows = client._decode_batch(payload, schema)

        assert len(rows) == 4
        assert rows[0]["b"] is True
        assert rows[1]["b"] is False
        assert rows[2]["b"] is True
        assert rows[3]["b"] is True


# ---------------------------------------------------------------------------
# BATCH decoding — DOUBLE
# ---------------------------------------------------------------------------


def _build_double_batch(
    values: list[float], col_name: str = "d"
) -> tuple[bytes, list[tuple[str, int]]]:
    """Build a BATCH payload with one DOUBLE column."""
    row_count = len(values)
    col_count = 1
    sv_len = 0

    batch = struct.pack("<IHI", row_count, col_count, sv_len)

    data = struct.pack(f"<{row_count}d", *values)
    data_length = len(data)

    batch += struct.pack("<I", data_length)

    current_offset = len(batch)
    padding = (8 - (current_offset % 8)) % 8
    batch += b"\x00" * padding

    batch += data

    bitmap_len = math.ceil(row_count / 8)
    bitmap = bytearray(bitmap_len)
    for i in range(row_count):
        bitmap[i // 8] |= 1 << (i % 8)
    batch += bytes(bitmap)

    schema = [(col_name, COL_DOUBLE)]
    return (batch, schema)


class TestDecodeDoubleBatch:
    def test_decode_double_column(self) -> None:
        """Decode a BATCH with one DOUBLE column."""
        payload, schema = _build_double_batch([3.14, -2.718, 0.0])
        client = XrayProtocolClient("localhost")
        rows = client._decode_batch(payload, schema)

        assert len(rows) == 3
        assert abs(rows[0]["d"] - 3.14) < 1e-10
        assert abs(rows[1]["d"] - (-2.718)) < 1e-10
        assert rows[2]["d"] == 0.0


# ---------------------------------------------------------------------------
# ERROR decoding
# ---------------------------------------------------------------------------


class TestDecodeError:
    def test_decode_error(self) -> None:
        """Decode an ERROR payload."""
        client = XrayProtocolClient("localhost")

        msg_text = "Syntax error near RETURN"
        detail_text = "Line 1, column 7"

        payload = struct.pack("<I", 1001)  # code
        payload += struct.pack("<B", 2)  # severity
        payload += struct.pack("<B", 1)  # retryable = True
        payload += struct.pack("<H", len(msg_text))
        payload += msg_text.encode("utf-8")
        payload += struct.pack("<H", len(detail_text))
        payload += detail_text.encode("utf-8")

        err = client._decode_error(payload)
        assert err["code"] == 1001
        assert err["severity"] == 2
        assert err["retryable"] is True
        assert err["message"] == msg_text
        assert err["detail"] == detail_text

    def test_decode_error_empty_detail(self) -> None:
        """Decode an ERROR with empty detail string."""
        client = XrayProtocolClient("localhost")

        msg_text = "Internal error"
        payload = struct.pack("<I", 500)
        payload += struct.pack("<B", 3)
        payload += struct.pack("<B", 0)
        payload += struct.pack("<H", len(msg_text))
        payload += msg_text.encode("utf-8")
        payload += struct.pack("<H", 0)

        err = client._decode_error(payload)
        assert err["code"] == 500
        assert err["retryable"] is False
        assert err["detail"] == ""


# ---------------------------------------------------------------------------
# Null bitmap
# ---------------------------------------------------------------------------


class TestNullBitmap:
    def test_all_valid(self) -> None:
        """All bits set: all values should survive."""
        values = [1, 2, 3, 4, 5, 6, 7, 8]
        bitmap = bytes([0xFF])  # all 8 bits set
        result = _apply_null_bitmap(values, bitmap, 8)
        assert result == values

    def test_all_null(self) -> None:
        """No bits set: all values should become None."""
        values = [1, 2, 3]
        bitmap = bytes([0b000])
        result = _apply_null_bitmap(values, bitmap, 3)
        assert result == [None, None, None]

    def test_mixed_nulls(self) -> None:
        """Some bits set, some not — verify LSB-first ordering."""
        # bitmap 0b00000101 = bits 0 and 2 are set (valid)
        values = [10, 20, 30]
        bitmap = bytes([0b101])
        result = _apply_null_bitmap(values, bitmap, 3)
        assert result == [10, None, 30]

    def test_multi_byte_bitmap(self) -> None:
        """Null bitmap spanning multiple bytes (9 rows)."""
        values = list(range(9))
        # byte 0: 0b11111111 (rows 0-7 all valid)
        # byte 1: 0b00000001 (row 8 valid)
        bitmap = bytes([0xFF, 0x01])
        result = _apply_null_bitmap(values, bitmap, 9)
        assert result == list(range(9))

    def test_multi_byte_bitmap_with_nulls(self) -> None:
        """9 rows, row 3 and row 8 are null."""
        values = list(range(9))
        # byte 0: 0b11110111 = rows 0,1,2 valid, row 3 null, rows 4-7 valid
        # byte 1: 0b00000000 = row 8 null
        bitmap = bytes([0b11110111, 0b00000000])
        result = _apply_null_bitmap(values, bitmap, 9)
        expected = [0, 1, 2, None, 4, 5, 6, 7, None]
        assert result == expected

    def test_empty_bitmap(self) -> None:
        """Zero rows should return empty list."""
        result = _apply_null_bitmap([], b"", 0)
        assert result == []


# ---------------------------------------------------------------------------
# Frame encoding
# ---------------------------------------------------------------------------


class TestEncodeFrame:
    def test_frame_header_size(self) -> None:
        """Frame header is exactly 8 bytes."""
        assert FRAME_HEADER_SIZE == 8

    def test_frame_roundtrip(self) -> None:
        """Encode a frame and manually decode the header."""
        payload = b"test data"
        frame = encode_frame(MSG_SCHEMA, 7, payload)

        plen, msg_type, flags, qid = struct.unpack_from("<IBBH", frame, 0)
        assert plen == len(payload)
        assert msg_type == MSG_SCHEMA
        assert flags == 0
        assert qid == 7
        assert frame[FRAME_HEADER_SIZE:] == payload

    def test_empty_payload_frame(self) -> None:
        """Frame with empty payload."""
        frame = encode_frame(MSG_COMPLETE, 0, b"")
        assert len(frame) == FRAME_HEADER_SIZE
        plen, msg_type, flags, qid = struct.unpack_from("<IBBH", frame, 0)
        assert plen == 0
        assert msg_type == MSG_COMPLETE


# ---------------------------------------------------------------------------
# Client construction (no network)
# ---------------------------------------------------------------------------


class TestClientConstruction:
    def test_defaults(self) -> None:
        """Client initializes with correct defaults."""
        client = XrayProtocolClient("myhost", port=7689)
        assert client._host == "myhost"
        assert client._port == 7689
        assert client.connected is False

    def test_not_connected_raises(self) -> None:
        """Operations on disconnected client should raise."""
        client = XrayProtocolClient("localhost")
        with pytest.raises(Exception):
            client.execute("RETURN 1")
        with pytest.raises(Exception):
            client.ping()
