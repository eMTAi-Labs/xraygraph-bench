"""Low-level binary protocol client for xrayGraphDB's columnar wire protocol.

Implements the xrayProtocol framing specification for communication with
xrayGraphDB on port 7689.  Handles HELLO handshake, EXECUTE queries,
SCHEMA/BATCH/COMPLETE response collection, and PING/PONG keepalives.

Frame format (8 bytes, little-endian):
    [4B payload_length][1B message_type][1B flags][2B query_id]

Column types:
    0x01=NULL  0x02=BOOL  0x03=INT64  0x04=DOUBLE  0x05=STRING
    0x06=LIST  0x07=MAP
"""

from __future__ import annotations

import math
import socket
import struct
from typing import Any

# ---------------------------------------------------------------------------
# Message type constants
# ---------------------------------------------------------------------------
MSG_HELLO = 0x01
MSG_HELLO_OK = 0x02
MSG_EXECUTE = 0x03
MSG_SCHEMA = 0x04
MSG_BATCH = 0x05
MSG_COMPLETE = 0x06
MSG_ERROR = 0x07
MSG_CANCEL = 0x08
MSG_PING = 0x0A
MSG_PONG = 0x0B

# ---------------------------------------------------------------------------
# Column type constants
# ---------------------------------------------------------------------------
COL_NULL = 0x01
COL_BOOL = 0x02
COL_INT64 = 0x03
COL_DOUBLE = 0x04
COL_STRING = 0x05
COL_LIST = 0x06
COL_MAP = 0x07

# ---------------------------------------------------------------------------
# Capability bits
# ---------------------------------------------------------------------------
CAP_LZ4 = 1 << 0
CAP_MULTIPLEX = 1 << 1
CAP_SELECTION_VECTORS = 1 << 2
CAP_PROJECTION_PUSHDOWN = 1 << 3
CAP_STREAMING_BACKPRESSURE = 1 << 4
CAP_GFQL_TEXT = 1 << 5

# ---------------------------------------------------------------------------
# Execute option bits
# ---------------------------------------------------------------------------
OPT_PROFILE = 1 << 0
OPT_EXPLAIN = 1 << 1
OPT_READ_ONLY = 1 << 2

# Frame header size
FRAME_HEADER_SIZE = 8

# Language codes
LANG_CYPHER = 0
LANG_GFQL = 1


class XrayProtocolError(Exception):
    """Raised when the xrayProtocol handshake or communication fails."""

    def __init__(
        self,
        message: str,
        code: int = 0,
        severity: int = 0,
        retryable: bool = False,
        detail: str = "",
    ):
        super().__init__(message)
        self.code = code
        self.severity = severity
        self.retryable = retryable
        self.detail = detail


class XrayProtocolClient:
    """Binary protocol client for xrayGraphDB's columnar wire protocol.

    Connects via TCP to port 7689 (default), performs the HELLO handshake,
    and supports executing Cypher/GFQL queries that return columnar BATCH
    results.
    """

    def __init__(self, host: str, port: int = 7689, timeout: float = 30.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._sock: socket.socket | None = None
        self._query_id_counter: int = 0
        self._server_version: int = 0
        self._server_caps: int = 0
        self._server_info: str = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def connect(
        self,
        username: str = "",
        password: str = "",
        capabilities: int = 0,
    ) -> tuple[int, int, str]:
        """TCP connect + HELLO handshake.

        Args:
            username: Authentication username.
            password: Authentication password.
            capabilities: Requested capability bitmask.

        Returns:
            Tuple of (protocol_version, negotiated_capabilities, server_info).

        Raises:
            XrayProtocolError: On handshake failure or unexpected response.
        """
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self._timeout)
        self._sock.connect((self._host, self._port))

        # Build HELLO payload
        auth_token = f"{username}:{password}".encode("utf-8")
        payload = struct.pack(
            "<HHI",
            1,  # protocol_version
            capabilities,
            len(auth_token),
        )
        payload += auth_token

        self._send_frame(MSG_HELLO, 0, payload)

        # Expect HELLO_OK
        msg_type, _flags, _qid, resp_payload = self._recv_frame()
        if msg_type == MSG_ERROR:
            err = self._decode_error(resp_payload)
            raise XrayProtocolError(
                err["message"],
                code=err["code"],
                severity=err["severity"],
                retryable=err["retryable"],
                detail=err["detail"],
            )
        if msg_type != MSG_HELLO_OK:
            raise XrayProtocolError(
                f"Expected HELLO_OK (0x02), got 0x{msg_type:02X}"
            )

        version, caps, info_len = struct.unpack_from("<HHI", resp_payload, 0)
        info_str = resp_payload[8 : 8 + info_len].decode("utf-8")

        self._server_version = version
        self._server_caps = caps
        self._server_info = info_str

        return (version, caps, info_str)

    def close(self) -> None:
        """Close TCP connection."""
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()
            self._sock = None

    def execute(
        self,
        query: str,
        language: int = LANG_CYPHER,
        params: dict[str, Any] | None = None,
        options: int = 0,
    ) -> tuple[list[tuple[str, int]], list[dict[str, Any]], int]:
        """Send EXECUTE, collect SCHEMA + BATCH* + COMPLETE.

        Args:
            query: Query string (Cypher or GFQL).
            language: 0=Cypher, 1=GFQL.
            params: Query parameters (reserved, currently unused).
            options: Bitmask (bit0=PROFILE, bit1=EXPLAIN, bit2=READ_ONLY).

        Returns:
            Tuple of (columns, rows, complete_flags) where:
            - columns: list of (name, type_code) tuples
            - rows: list of dicts {col_name: value}
            - complete_flags: int (bit0=had_error, bit1=cancelled)

        Raises:
            XrayProtocolError: On server error or protocol violation.
        """
        if self._sock is None:
            raise XrayProtocolError("Not connected")

        self._query_id_counter += 1
        qid = self._query_id_counter & 0xFFFF

        # Build EXECUTE payload
        query_bytes = query.encode("utf-8")
        payload = struct.pack(
            "<BIIHI",
            language,
            len(query_bytes),
            0,  # parameter_count
            0,  # projection_count (0=all)
            options,
        )
        # Fix: parameter_count is u32, options is u32, projection_count is u16
        # Re-pack with correct layout: u8 language, u32 query_length, N query,
        # u32 parameter_count, u32 options, u16 projection_count
        payload = struct.pack("<B", language)
        payload += struct.pack("<I", len(query_bytes))
        payload += query_bytes
        payload += struct.pack("<I", 0)  # parameter_count
        payload += struct.pack("<I", options)
        payload += struct.pack("<H", 0)  # projection_count

        self._send_frame(MSG_EXECUTE, qid, payload)

        # Collect response frames: SCHEMA, then BATCH*, then COMPLETE
        columns: list[tuple[str, int]] = []
        rows: list[dict[str, Any]] = []
        complete_flags: int = 0

        while True:
            msg_type, _flags, _resp_qid, resp_payload = self._recv_frame()

            if msg_type == MSG_SCHEMA:
                columns = self._decode_schema(resp_payload)

            elif msg_type == MSG_BATCH:
                batch_rows = self._decode_batch(resp_payload, columns)
                rows.extend(batch_rows)

            elif msg_type == MSG_COMPLETE:
                if len(resp_payload) >= 1:
                    complete_flags = resp_payload[0]
                break

            elif msg_type == MSG_ERROR:
                err = self._decode_error(resp_payload)
                raise XrayProtocolError(
                    err["message"],
                    code=err["code"],
                    severity=err["severity"],
                    retryable=err["retryable"],
                    detail=err["detail"],
                )
            else:
                raise XrayProtocolError(
                    f"Unexpected message type 0x{msg_type:02X} during query"
                )

        return (columns, rows, complete_flags)

    def ping(self) -> None:
        """Send PING, wait for PONG.

        Raises:
            XrayProtocolError: If PONG is not received.
        """
        if self._sock is None:
            raise XrayProtocolError("Not connected")

        self._send_frame(MSG_PING, 0, b"")
        msg_type, _flags, _qid, _payload = self._recv_frame()
        if msg_type != MSG_PONG:
            raise XrayProtocolError(
                f"Expected PONG (0x0B), got 0x{msg_type:02X}"
            )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def server_version(self) -> int:
        return self._server_version

    @property
    def server_capabilities(self) -> int:
        return self._server_caps

    @property
    def server_info(self) -> str:
        return self._server_info

    @property
    def connected(self) -> bool:
        return self._sock is not None

    # ------------------------------------------------------------------
    # Internal: frame I/O
    # ------------------------------------------------------------------

    def _send_frame(self, msg_type: int, query_id: int, payload: bytes) -> None:
        """Send a framed message over the TCP socket.

        Args:
            msg_type: Message type byte.
            query_id: 16-bit query identifier.
            payload: Raw payload bytes.
        """
        header = struct.pack(
            "<IBBH",
            len(payload),
            msg_type,
            0,  # flags
            query_id,
        )
        if self._sock is None:
            raise XrayProtocolError("Not connected")
        self._sock.sendall(header + payload)

    def _recv_frame(self) -> tuple[int, int, int, bytes]:
        """Receive and decode a single framed message.

        Returns:
            Tuple of (msg_type, flags, query_id, payload).
        """
        header = self._recv_exact(FRAME_HEADER_SIZE)
        payload_length, msg_type, flags, query_id = struct.unpack(
            "<IBBH", header
        )
        payload = self._recv_exact(payload_length) if payload_length > 0 else b""
        return (msg_type, flags, query_id, payload)

    def _recv_exact(self, n: int) -> bytes:
        """Read exactly *n* bytes from the socket.

        Args:
            n: Number of bytes to read.

        Returns:
            Exactly n bytes.

        Raises:
            XrayProtocolError: On connection close or timeout.
        """
        if self._sock is None:
            raise XrayProtocolError("Not connected")
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise XrayProtocolError(
                    f"Connection closed (read {len(buf)} of {n} bytes)"
                )
            buf.extend(chunk)
        return bytes(buf)

    # ------------------------------------------------------------------
    # Internal: payload decoders
    # ------------------------------------------------------------------

    def _decode_schema(self, payload: bytes) -> list[tuple[str, int]]:
        """Decode a SCHEMA payload into column definitions.

        Args:
            payload: Raw SCHEMA payload bytes.

        Returns:
            List of (column_name, column_type_code) tuples.
        """
        offset = 0
        (col_count,) = struct.unpack_from("<H", payload, offset)
        offset += 2

        columns: list[tuple[str, int]] = []
        for _ in range(col_count):
            col_type = payload[offset]
            offset += 1
            (name_len,) = struct.unpack_from("<H", payload, offset)
            offset += 2
            name = payload[offset : offset + name_len].decode("utf-8")
            offset += name_len
            columns.append((name, col_type))

        return columns

    def _decode_batch(
        self,
        payload: bytes,
        schema: list[tuple[str, int]],
    ) -> list[dict[str, Any]]:
        """Decode a BATCH payload into row dicts using the schema.

        Args:
            payload: Raw BATCH payload bytes.
            schema: Column definitions from the SCHEMA message.

        Returns:
            List of dicts, one per row, keyed by column name.
        """
        offset = 0
        (row_count,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        (col_count,) = struct.unpack_from("<H", payload, offset)
        offset += 2
        (sv_length,) = struct.unpack_from("<I", payload, offset)
        offset += 4

        # Selection vector (skip for now — we don't filter)
        if sv_length > 0:
            offset += sv_length * 2  # u16 per entry

        # Decode each column
        all_col_values: list[list[Any]] = []
        for col_idx in range(col_count):
            col_name, col_type = schema[col_idx] if col_idx < len(schema) else (f"col{col_idx}", COL_NULL)
            values, offset = self._decode_column(
                payload, offset, col_type, row_count
            )
            all_col_values.append(values)

        # Transpose columns into rows
        rows: list[dict[str, Any]] = []
        for row_idx in range(row_count):
            row: dict[str, Any] = {}
            for col_idx in range(col_count):
                col_name = schema[col_idx][0] if col_idx < len(schema) else f"col{col_idx}"
                row[col_name] = all_col_values[col_idx][row_idx]
            rows.append(row)

        return rows

    def _decode_column(
        self,
        payload: bytes,
        offset: int,
        col_type: int,
        row_count: int,
    ) -> tuple[list[Any], int]:
        """Dispatch to the appropriate column decoder.

        Args:
            payload: Raw BATCH payload bytes.
            offset: Current read offset into payload.
            col_type: Column type code.
            row_count: Number of rows in this batch.

        Returns:
            Tuple of (values list, new offset after column data + null bitmap).
        """
        if col_type == COL_INT64:
            return self._decode_int64_column(payload, offset, row_count)
        elif col_type == COL_DOUBLE:
            return self._decode_double_column(payload, offset, row_count)
        elif col_type == COL_BOOL:
            return self._decode_bool_column(payload, offset, row_count)
        elif col_type == COL_STRING:
            return self._decode_string_column(payload, offset, row_count)
        elif col_type == COL_NULL:
            return self._decode_null_column(payload, offset, row_count)
        else:
            # Unknown column type — skip data_length bytes + null bitmap
            (data_length,) = struct.unpack_from("<I", payload, offset)
            offset += 4
            # Align to 8 bytes
            padding = (8 - (offset % 8)) % 8
            offset += padding
            offset += data_length
            # Null bitmap
            bitmap_len = math.ceil(row_count / 8)
            offset += bitmap_len
            return ([None] * row_count, offset)

    def _decode_int64_column(
        self, payload: bytes, offset: int, row_count: int
    ) -> tuple[list[Any], int]:
        """Decode an INT64 column (8 bytes per row, little-endian signed).

        Returns:
            Tuple of (values, new_offset).
        """
        (data_length,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        # Align to 8 bytes
        padding = (8 - (offset % 8)) % 8
        offset += padding

        values: list[Any] = []
        for i in range(row_count):
            (val,) = struct.unpack_from("<q", payload, offset + i * 8)
            values.append(val)
        offset += data_length

        # Null bitmap
        bitmap_len = math.ceil(row_count / 8)
        bitmap = payload[offset : offset + bitmap_len]
        offset += bitmap_len

        # Apply null bitmap (bit=1 means valid, LSB first)
        values = _apply_null_bitmap(values, bitmap, row_count)
        return (values, offset)

    def _decode_double_column(
        self, payload: bytes, offset: int, row_count: int
    ) -> tuple[list[Any], int]:
        """Decode a DOUBLE column (8 bytes per row, little-endian IEEE754).

        Returns:
            Tuple of (values, new_offset).
        """
        (data_length,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        padding = (8 - (offset % 8)) % 8
        offset += padding

        values: list[Any] = []
        for i in range(row_count):
            (val,) = struct.unpack_from("<d", payload, offset + i * 8)
            values.append(val)
        offset += data_length

        bitmap_len = math.ceil(row_count / 8)
        bitmap = payload[offset : offset + bitmap_len]
        offset += bitmap_len

        values = _apply_null_bitmap(values, bitmap, row_count)
        return (values, offset)

    def _decode_bool_column(
        self, payload: bytes, offset: int, row_count: int
    ) -> tuple[list[Any], int]:
        """Decode a BOOL column (1 byte per row).

        Returns:
            Tuple of (values, new_offset).
        """
        (data_length,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        padding = (8 - (offset % 8)) % 8
        offset += padding

        values: list[Any] = []
        for i in range(row_count):
            values.append(bool(payload[offset + i]))
        offset += data_length

        bitmap_len = math.ceil(row_count / 8)
        bitmap = payload[offset : offset + bitmap_len]
        offset += bitmap_len

        values = _apply_null_bitmap(values, bitmap, row_count)
        return (values, offset)

    def _decode_string_column(
        self, payload: bytes, offset: int, row_count: int
    ) -> tuple[list[Any], int]:
        """Decode a STRING column (offset array + blob).

        Layout:
            u32 total_string_bytes
            u32[row_count+1] offsets
            bytes[total_string_bytes] string_data

        Returns:
            Tuple of (values, new_offset).
        """
        (data_length,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        padding = (8 - (offset % 8)) % 8
        offset += padding

        data_start = offset

        (total_string_bytes,) = struct.unpack_from("<I", payload, offset)
        offset += 4

        # Read row_count + 1 offset values
        offsets: list[int] = []
        for i in range(row_count + 1):
            (off,) = struct.unpack_from("<I", payload, offset)
            offsets.append(off)
            offset += 4

        string_data_start = offset
        values: list[Any] = []
        for i in range(row_count):
            start = offsets[i]
            end = offsets[i + 1]
            val = payload[string_data_start + start : string_data_start + end].decode(
                "utf-8"
            )
            values.append(val)

        offset = data_start + data_length

        bitmap_len = math.ceil(row_count / 8)
        bitmap = payload[offset : offset + bitmap_len]
        offset += bitmap_len

        values = _apply_null_bitmap(values, bitmap, row_count)
        return (values, offset)

    def _decode_null_column(
        self, payload: bytes, offset: int, row_count: int
    ) -> tuple[list[Any], int]:
        """Decode a NULL column (no data, all values are None).

        Returns:
            Tuple of (values, new_offset).
        """
        (data_length,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        padding = (8 - (offset % 8)) % 8
        offset += padding
        offset += data_length

        bitmap_len = math.ceil(row_count / 8)
        offset += bitmap_len

        return ([None] * row_count, offset)

    def _decode_error(self, payload: bytes) -> dict[str, Any]:
        """Decode an ERROR payload.

        Layout:
            u32 code
            u8  severity
            u8  retryable
            u16 message_length
            N   message (UTF-8)
            u16 detail_length
            N   detail (UTF-8)

        Returns:
            Dict with keys: code, severity, retryable, message, detail.
        """
        offset = 0
        (code,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        severity = payload[offset]
        offset += 1
        retryable = bool(payload[offset])
        offset += 1
        (msg_len,) = struct.unpack_from("<H", payload, offset)
        offset += 2
        message = payload[offset : offset + msg_len].decode("utf-8")
        offset += msg_len
        (detail_len,) = struct.unpack_from("<H", payload, offset)
        offset += 2
        detail = payload[offset : offset + detail_len].decode("utf-8")

        return {
            "code": code,
            "severity": severity,
            "retryable": retryable,
            "message": message,
            "detail": detail,
        }


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _apply_null_bitmap(
    values: list[Any], bitmap: bytes, row_count: int
) -> list[Any]:
    """Apply the null bitmap to a list of values.

    In the xrayProtocol null bitmap, bit=1 means the value is valid (non-null),
    bit=0 means null.  Bits are read LSB first within each byte.

    Args:
        values: Decoded values (may contain garbage for null positions).
        bitmap: Raw bitmap bytes.
        row_count: Number of rows.

    Returns:
        New values list with None in null positions.
    """
    result: list[Any] = []
    for i in range(row_count):
        byte_idx = i // 8
        bit_idx = i % 8
        if byte_idx < len(bitmap) and (bitmap[byte_idx] >> bit_idx) & 1:
            result.append(values[i])
        else:
            result.append(None)
    return result


def encode_hello_payload(
    username: str = "",
    password: str = "",
    capabilities: int = 0,
) -> bytes:
    """Build a HELLO payload (useful for testing).

    Args:
        username: Auth username.
        password: Auth password.
        capabilities: Requested capability bitmask.

    Returns:
        HELLO payload bytes.
    """
    auth_token = f"{username}:{password}".encode("utf-8")
    payload = struct.pack("<HHI", 1, capabilities, len(auth_token))
    payload += auth_token
    return payload


def encode_execute_payload(
    query: str,
    language: int = LANG_CYPHER,
    options: int = 0,
) -> bytes:
    """Build an EXECUTE payload (useful for testing).

    Args:
        query: Query string.
        language: Language code (0=Cypher, 1=GFQL).
        options: Execute options bitmask.

    Returns:
        EXECUTE payload bytes.
    """
    query_bytes = query.encode("utf-8")
    payload = struct.pack("<B", language)
    payload += struct.pack("<I", len(query_bytes))
    payload += query_bytes
    payload += struct.pack("<I", 0)  # parameter_count
    payload += struct.pack("<I", options)
    payload += struct.pack("<H", 0)  # projection_count
    return payload


def encode_frame(msg_type: int, query_id: int, payload: bytes) -> bytes:
    """Encode a complete frame (header + payload).

    Args:
        msg_type: Message type byte.
        query_id: 16-bit query identifier.
        payload: Payload bytes.

    Returns:
        Complete frame bytes (header + payload).
    """
    header = struct.pack("<IBBH", len(payload), msg_type, 0, query_id)
    return header + payload
