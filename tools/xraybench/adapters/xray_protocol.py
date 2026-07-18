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
COL_NODE = 0x08
COL_RELATIONSHIP = 0x09
COL_PATH = 0x0A
COL_BYTES = 0x0B
# Temporal / spatial — variable-width offset+blob columns (NOT fixed-width;
# the engine's IsComplexType_ routes 0x0C-0x12 through the offset+blob path,
# each cell body = the recursive-Value body for that tag).
COL_DATE = 0x0C
COL_LOCAL_TIME = 0x0D
COL_LOCAL_DATE_TIME = 0x0E
COL_ZONED_DATE_TIME = 0x0F
COL_DURATION = 0x10
COL_POINT_2D = 0x11
COL_POINT_3D = 0x12

# Column types whose body is [u32 blob_size][u32 offsets[row_count+1]][blob]
# with each cell decoded by a per-type body reader (shared with the recursive
# Value decoder). Same outer envelope as STRING.
_OFFSETBLOB_COL_TYPES = frozenset(
    {
        COL_LIST,
        COL_MAP,
        COL_NODE,
        COL_RELATIONSHIP,
        COL_PATH,
        COL_BYTES,
        COL_DATE,
        COL_LOCAL_TIME,
        COL_LOCAL_DATE_TIME,
        COL_ZONED_DATE_TIME,
        COL_DURATION,
        COL_POINT_2D,
        COL_POINT_3D,
    }
)

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
        database: str = "",
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
            2,  # protocol_version (v2 — current wire; query surfaces compatible)
            capabilities,
            len(auth_token),
        )
        payload += auth_token
        # v2: the database field is MANDATORY — the server rejects HELLO without it.
        db_bytes = database.encode("utf-8")
        payload += struct.pack("<I", len(db_bytes)) + db_bytes

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

    # ------------------------------------------------------------------
    # Bulk insert API
    # ------------------------------------------------------------------

    def bulk_begin(self, fmt: int = 0, hint: str = "", flags: int = 0) -> None:
        """Send BULK_INSERT_BEGIN to start a bulk session."""
        hint_bytes = hint.encode("utf-8")
        payload = struct.pack("<B", fmt)
        payload += struct.pack("<I", len(hint_bytes)) + hint_bytes
        payload += struct.pack("<I", flags)
        self._send_frame(0x20, 0, payload)
        msg_type, _, _, resp = self._recv_frame()
        if msg_type == 0x26:  # BULK_INSERT_ERROR
            err = self._decode_error(resp)
            raise XrayProtocolError(f"BULK_BEGIN error: {err['message']}")

    def bulk_upsert_nodes(
        self,
        label: str,
        key_col: str,
        rows: list[dict[str, Any]],
    ) -> tuple[int, int]:
        """Send BULK_UPSERT_NODES batch.

        Wire format (from executor_bridge.hpp):
          u32 node_count
          u32_len + string key_name
          u32 prop_count
          [prop_count] u32_len + string prop_name
          u32 label_count
          [label_count] u32_len + string label_name
          [node_count]:
            u32_len + string key_value
            [prop_count] typed_value (tag byte + value)

        Type tags: 0=string, 1=int64, 2=double, 3=bool, 4=null
        Legacy (tag > 4): all-string (u32_len + string)

        Returns (nodes_created, time_ms).
        """
        if not rows:
            return (0, 0)

        # Collect property names (excluding key)
        prop_names: list[str] = []
        for row in rows:
            for k in row:
                if k != key_col and k not in prop_names:
                    prop_names.append(k)

        def write_string(b: bytearray, s: str) -> None:
            sb = s.encode("utf-8")
            b += struct.pack("<I", len(sb))
            b += sb

        def write_typed(b: bytearray, val: Any) -> None:
            if val is None:
                b += struct.pack("<B", 4)  # null tag
            elif isinstance(val, bool):
                b += struct.pack("<B", 3)  # bool tag
                b += struct.pack("<B", 1 if val else 0)
            elif isinstance(val, int):
                b += struct.pack("<B", 1)  # int64 tag
                b += struct.pack("<q", val)
            elif isinstance(val, float):
                b += struct.pack("<B", 2)  # double tag
                b += struct.pack("<d", val)
            else:
                b += struct.pack("<B", 0)  # string tag
                sb = str(val).encode("utf-8")
                b += struct.pack("<I", len(sb))
                b += sb

        buf = bytearray()
        # node_count
        buf += struct.pack("<I", len(rows))
        # key_name
        write_string(buf, key_col)
        # prop_count
        buf += struct.pack("<I", len(prop_names))
        # prop_names
        for pn in prop_names:
            write_string(buf, pn)
        # label_count + labels
        buf += struct.pack("<I", 1)
        write_string(buf, label)
        # per-node data
        for row in rows:
            write_string(buf, str(row[key_col]))
            for pn in prop_names:
                write_typed(buf, row.get(pn))

        self._send_frame(0x27, 0, bytes(buf))
        msg_type, _, _, resp = self._recv_frame()
        if msg_type == 0x26:  # ERROR
            err = self._decode_error(resp)
            raise XrayProtocolError(f"BULK_UPSERT error: {err['message']}")
        if msg_type == 0x25 and len(resp) >= 12:  # ACK
            nodes = struct.unpack_from("<I", resp, 0)[0]
            time_ms = struct.unpack_from("<I", resp, 8)[0]
            return (nodes, time_ms)
        return (0, 0)

    def bulk_insert_edges(
        self,
        edges: list[dict[str, str]],
        prop_names: list[str] | None = None,
    ) -> tuple[int, int]:
        """Send BULK_INSERT_EDGES batch.

        Each edge dict must have 'from', 'to', 'type' keys,
        plus any property values matching prop_names.

        Returns (edges_created, time_ms).
        """
        if not edges:
            return (0, 0)

        prop_names = prop_names or []
        edge_count = len(edges)
        prop_count = len(prop_names)

        payload = struct.pack("<II", edge_count, prop_count)

        # Property name strings
        for pn in prop_names:
            pn_bytes = pn.encode("utf-8")
            payload += struct.pack("<I", len(pn_bytes)) + pn_bytes

        # Per-edge data: from_fnid, to_fnid, edge_type, [prop_values]
        for edge in edges:
            for field in ["from", "to", "type"]:
                val = str(edge[field]).encode("utf-8")
                payload += struct.pack("<I", len(val)) + val
            for pn in prop_names:
                val = str(edge.get(pn, "")).encode("utf-8")
                payload += struct.pack("<I", len(val)) + val

        self._send_frame(0x22, 0, payload)
        msg_type, _, _, resp = self._recv_frame()
        if msg_type == 0x26:  # ERROR
            err = self._decode_error(resp)
            raise XrayProtocolError(f"BULK_INSERT_EDGES error: {err['message']}")
        if msg_type == 0x25:  # ACK: [u32 count][f64 ms]
            edges_created = struct.unpack_from("<I", resp, 0)[0]
            time_ms = int(struct.unpack_from("<d", resp, 4)[0])
            return (edges_created, time_ms)
        return (0, 0)

    def bulk_insert_edges_keyed(
        self,
        left_label: str,
        left_key: str,
        right_label: str,
        right_key: str,
        edge_type: str,
        edges: list[dict[str, Any]],
        prop_names: list[str] | None = None,
    ) -> tuple[int, int]:
        """Send BULK_INSERT_EDGES_KEYED (0x30) batch.

        Generalizes 0x22: endpoints are matched by an explicit per-batch
        key property (left_label.left_key / right_label.right_key) instead
        of the hardcoded "fnid". Both (label, key) pairs MUST have a
        property index — bulk_upsert_nodes auto-creates one on the key col.

        Each edge dict supplies 'from' (left key value) and 'to' (right
        key value), plus any property values matching prop_names. Endpoint
        key values are normalized to str(int(v)) to match the node-upsert
        key encoding so the string-typed index probe resolves.

        Returns (edges_created, time_ms).
        """
        if not edges:
            return (0, 0)

        prop_names = prop_names or []
        edge_count = len(edges)
        prop_count = len(prop_names)

        def write_string(b: bytearray, s: Any) -> None:
            sb = str(s).encode("utf-8")
            b += struct.pack("<I", len(sb))
            b += sb

        def norm_key(v: Any) -> str:
            # Match the node-upsert key encoding str(int(id)) (no leading
            # zeros / decimals). LDBC ids are integers; fall back to the raw
            # string for any non-numeric key.
            try:
                return str(int(v))
            except (TypeError, ValueError):
                return str(v)

        payload = bytearray()
        # Header: left_label, left_key, right_label, right_key, edge_type
        write_string(payload, left_label)
        write_string(payload, left_key)
        write_string(payload, right_label)
        write_string(payload, right_key)
        write_string(payload, edge_type)
        payload += struct.pack("<II", edge_count, prop_count)
        for pn in prop_names:
            write_string(payload, pn)
        # Per-edge body: left_key_value, right_key_value, [prop_values]
        for edge in edges:
            write_string(payload, norm_key(edge["from"]))
            write_string(payload, norm_key(edge["to"]))
            for pn in prop_names:
                write_string(payload, str(edge.get(pn, "")))

        self._send_frame(0x30, 0, bytes(payload))
        msg_type, _, _, resp = self._recv_frame()
        if msg_type == 0x26:  # ERROR
            err = self._decode_error(resp)
            raise XrayProtocolError(
                f"BULK_INSERT_EDGES_KEYED error: {err['message']}")
        if msg_type == 0x25 and len(resp) >= 12:  # ACK: [u32 count][f64 ms]
            edges_created = struct.unpack_from("<I", resp, 0)[0]
            time_ms = int(struct.unpack_from("<d", resp, 4)[0])
            return (edges_created, time_ms)
        return (0, 0)

    def bulk_commit(self) -> tuple[int, int, int]:
        """Send BULK_INSERT_COMMIT. Returns (nodes, edges, time_ms)."""
        self._send_frame(0x24, 0, b"")
        msg_type, _, _, resp = self._recv_frame()
        if msg_type == 0x26:
            err = self._decode_error(resp)
            raise XrayProtocolError(f"BULK_COMMIT error: {err['message']}")
        if msg_type == 0x25 and len(resp) >= 12:
            n = struct.unpack_from("<I", resp, 0)[0]
            e = struct.unpack_from("<I", resp, 4)[0]
            t = struct.unpack_from("<I", resp, 8)[0]
            return (n, e, t)
        return (0, 0, 0)

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
        elif col_type in _OFFSETBLOB_COL_TYPES:
            # NODE/REL/PATH/LIST/MAP/BYTES/temporal — variable-width columns
            # sharing the STRING envelope; each cell has a per-type body.
            return self._decode_offsetblob_column(
                payload, offset, row_count, _CELL_READERS[col_type]
            )
        else:
            # A correctness-validation harness must NEVER silently return None
            # for an unknown column (that is the exact P0 this decoder fixes):
            # a CAP_TYPED_NESTED (0x13-0x1A) or CAP_DICT_ENCODING column, or a
            # future tag, would validate benchmark results against nothing.
            # Fail loudly instead. Extend the client if this fires.
            raise XrayProtocolError(
                f"xrayProtocol: unhandled column type 0x{col_type:02X} — "
                f"the client decoder must be extended (did the connection "
                f"negotiate CAP_TYPED_NESTED / CAP_DICT_ENCODING?)."
            )

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

    def _decode_offsetblob_column(
        self,
        payload: bytes,
        offset: int,
        row_count: int,
        cell_reader: Any,
    ) -> tuple[list[Any], int]:
        """Decode a variable-width column (NODE/REL/PATH/LIST/MAP/BYTES/temporal).

        Outer envelope is byte-identical to STRING (confirmed against the
        engine encoder.hpp / executor_bridge.hpp complex-column writer):

            u32 data_length
            <pad current offset up to a multiple of 8>
            u32 blob_size
            u32 offsets[row_count + 1]
            blob[blob_size]
            u8[ceil(row_count/8)] null_bitmap

        A null (or empty) row is a ZERO-LENGTH cell
        (``offsets[r] == offsets[r+1]``) — it decodes to ``None`` WITHOUT
        invoking ``cell_reader`` (an empty slice would crash the i64/u32
        reads). ``cell_reader(cell_bytes) -> (value, consumed)`` must consume
        EXACTLY ``len(cell_bytes)``; a mismatch means a field-width drift vs
        the engine and is raised loudly rather than silently dropping bytes.

        Advancement is always ``data_start + data_length`` then the bitmap —
        never derived from the parsed body — so a decode error in one cell
        can never desync the following columns.
        """
        (data_length,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        padding = (8 - (offset % 8)) % 8
        offset += padding
        data_start = offset

        (_blob_size,) = struct.unpack_from("<I", payload, offset)
        offset += 4
        offsets: list[int] = []
        for _ in range(row_count + 1):
            (o,) = struct.unpack_from("<I", payload, offset)
            offsets.append(o)
            offset += 4

        blob_start = offset
        values: list[Any] = []
        for r in range(row_count):
            start = offsets[r]
            end = offsets[r + 1]
            if start == end:
                values.append(None)  # null / empty cell — masked by bitmap
                continue
            cell = payload[blob_start + start : blob_start + end]
            value, consumed = cell_reader(cell)
            if consumed != len(cell):
                raise XrayProtocolError(
                    f"xrayProtocol: cell decoder consumed {consumed} of "
                    f"{len(cell)} bytes at row {r} — field-width drift."
                )
            values.append(value)

        # Advance past the whole column body, then the null bitmap.
        offset = data_start + data_length
        bitmap_len = math.ceil(row_count / 8)
        bitmap = payload[offset : offset + bitmap_len]
        offset += bitmap_len

        values = _apply_null_bitmap(values, bitmap, row_count)
        return (values, offset)

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


# ---------------------------------------------------------------------------
# Complex-cell body decoders (NODE / RELATIONSHIP / PATH / LIST / MAP / BYTES /
# temporal) and the recursive tagged Value decoder.
#
# Ground truth: engine SSOT src/communication/xray/{executor_bridge,encoder,
# protocol}.hpp on .187. Each returns (value, new_index).  "Body" readers
# decode a cell that carries NO leading type tag (the column type or the
# recursive-Value tag already selected the shape).  _read_value decodes a
# TAGGED value (u8 tag + body) and is used for node/rel property values and
# list/map elements.
#
# Width contract (verified byte-for-byte against the encoder):
#   id / start / end        i64 LE (signed, WriteI64)
#   label / type / key len  u16 LE (WriteString)
#   all counts / offsets    u32 LE
#   recursive STRING len    u32 LE (WriteLongString)
#   PATH seq step           i32 LE (signed, 1-based; <0 = reverse)
# ---------------------------------------------------------------------------


def _read_string_short(buf: bytes, i: int) -> tuple[str, int]:
    """u16 LE length + UTF-8 bytes (engine WriteString)."""
    (n,) = struct.unpack_from("<H", buf, i)
    i += 2
    return (buf[i : i + n].decode("utf-8"), i + n)


def _read_bytes_body(buf: bytes, i: int) -> tuple[bytes, int]:
    """u32 LE length + raw octets (BYTES 0x0B).

    SPECULATIVE: there is no BYTES arm in the engine's EncodeTypedValueBody,
    so the server does not emit this on the columnar path today — this branch
    is spec-correct but cannot be validated against a live engine.
    """
    (n,) = struct.unpack_from("<I", buf, i)
    i += 4
    return (bytes(buf[i : i + n]), i + n)


def _read_node_body(buf: bytes, i: int) -> tuple[dict[str, Any], int]:
    (gid,) = struct.unpack_from("<q", buf, i)
    i += 8
    (label_count,) = struct.unpack_from("<I", buf, i)
    i += 4
    labels: list[str] = []
    for _ in range(label_count):
        lbl, i = _read_string_short(buf, i)
        labels.append(lbl)
    (prop_count,) = struct.unpack_from("<I", buf, i)
    i += 4
    props: dict[str, Any] = {}
    for _ in range(prop_count):
        key, i = _read_string_short(buf, i)
        val, i = _read_value(buf, i)
        props[key] = val
    return ({"id": gid, "labels": labels, "properties": props}, i)


def _read_rel_body(buf: bytes, i: int) -> tuple[dict[str, Any], int]:
    rid, start, end = struct.unpack_from("<qqq", buf, i)
    i += 24
    rtype, i = _read_string_short(buf, i)
    (prop_count,) = struct.unpack_from("<I", buf, i)
    i += 4
    props: dict[str, Any] = {}
    for _ in range(prop_count):
        key, i = _read_string_short(buf, i)
        val, i = _read_value(buf, i)
        props[key] = val
    return (
        {"id": rid, "start": start, "end": end, "type": rtype, "properties": props},
        i,
    )


def _read_path_body(buf: bytes, i: int) -> tuple[dict[str, Any], int]:
    (node_count,) = struct.unpack_from("<I", buf, i)
    i += 4
    nodes: list[Any] = []
    for _ in range(node_count):
        node, i = _read_node_body(buf, i)
        nodes.append(node)
    (rel_count,) = struct.unpack_from("<I", buf, i)
    i += 4
    rels: list[Any] = []
    for _ in range(rel_count):
        rel, i = _read_rel_body(buf, i)
        rels.append(rel)
    (seq_count,) = struct.unpack_from("<I", buf, i)
    i += 4
    seq: list[int] = []
    for _ in range(seq_count):
        (step,) = struct.unpack_from("<i", buf, i)
        i += 4
        seq.append(step)
    return ({"nodes": nodes, "relationships": rels, "sequence": seq}, i)


def _read_list_body(buf: bytes, i: int) -> tuple[list[Any], int]:
    (count,) = struct.unpack_from("<I", buf, i)
    i += 4
    out: list[Any] = []
    for _ in range(count):
        val, i = _read_value(buf, i)
        out.append(val)
    return (out, i)


def _read_map_body(buf: bytes, i: int) -> tuple[dict[str, Any], int]:
    (count,) = struct.unpack_from("<I", buf, i)
    i += 4
    out: dict[str, Any] = {}
    for _ in range(count):
        key, i = _read_string_short(buf, i)
        val, i = _read_value(buf, i)
        out[key] = val
    return (out, i)


def _read_value(buf: bytes, i: int) -> tuple[Any, int]:
    """Recursive tagged Value: u8 type_tag + body. Returns (value, new_index).

    XNULL (0x01) has NO body — return None and consume only the tag byte
    (Graph/Function/Enum also map to XNULL on the wire).
    """
    tag = buf[i]
    i += 1
    if tag == COL_NULL:  # 0x01 — no body
        return (None, i)
    if tag == COL_BOOL:  # 0x02
        return (buf[i] != 0, i + 1)
    if tag == COL_INT64:  # 0x03
        (v,) = struct.unpack_from("<q", buf, i)
        return (v, i + 8)
    if tag == COL_DOUBLE:  # 0x04
        (v,) = struct.unpack_from("<d", buf, i)
        return (v, i + 8)
    if tag == COL_STRING:  # 0x05 — recursive LongString (u32 len + bytes)
        (n,) = struct.unpack_from("<I", buf, i)
        i += 4
        return (buf[i : i + n].decode("utf-8"), i + n)
    if tag == COL_LIST:  # 0x06
        return _read_list_body(buf, i)
    if tag == COL_MAP:  # 0x07
        return _read_map_body(buf, i)
    if tag == COL_NODE:  # 0x08
        return _read_node_body(buf, i)
    if tag == COL_RELATIONSHIP:  # 0x09
        return _read_rel_body(buf, i)
    if tag == COL_PATH:  # 0x0A
        return _read_path_body(buf, i)
    if tag == COL_BYTES:  # 0x0B
        return _read_bytes_body(buf, i)
    if tag == COL_DATE:  # 0x0C — i64 days
        (v,) = struct.unpack_from("<q", buf, i)
        return (v, i + 8)
    if tag == COL_LOCAL_TIME:  # 0x0D — i64 ns
        (v,) = struct.unpack_from("<q", buf, i)
        return (v, i + 8)
    if tag == COL_LOCAL_DATE_TIME:  # 0x0E — i64 epoch_s + i32 ns
        s, ns = struct.unpack_from("<qi", buf, i)
        return ((s, ns), i + 12)
    if tag == COL_ZONED_DATE_TIME:  # 0x0F — i64 epoch_s + i32 ns + i32 tz_off_s
        s, ns, tz = struct.unpack_from("<qii", buf, i)
        return ((s, ns, tz), i + 16)
    if tag == COL_DURATION:  # 0x10 — i64 total_s + i64 ns
        s, ns = struct.unpack_from("<qq", buf, i)
        return ((s, ns), i + 16)
    if tag == COL_POINT_2D:  # 0x11 — i32 srid + f64 x + f64 y
        srid, x, y = struct.unpack_from("<idd", buf, i)
        return ((srid, x, y), i + 20)
    if tag == COL_POINT_3D:  # 0x12 — i32 srid + f64 x + f64 y + f64 z
        srid, x, y, z = struct.unpack_from("<iddd", buf, i)
        return ((srid, x, y, z), i + 28)
    raise XrayProtocolError(
        f"xrayProtocol: unhandled recursive Value tag 0x{tag:02X}"
    )


def _temporal_reader(fmt: str, size: int) -> Any:
    """Build a column cell reader for a fixed-layout temporal/spatial body
    (no tag, no inner length). Returns the scalar for single-field forms and
    a tuple otherwise; always reports ``size`` bytes consumed.
    """

    def read(cell: bytes) -> tuple[Any, int]:
        vals = struct.unpack_from(fmt, cell, 0)
        return (vals[0] if len(vals) == 1 else vals, size)

    return read


# Column-type → cell body reader. Each reader takes the raw cell bytes and
# returns (value, consumed); _decode_offsetblob_column asserts consumed ==
# len(cell). LIST/MAP/NODE/REL/PATH/BYTES bodies start at index 0.
_CELL_READERS: dict[int, Any] = {
    COL_LIST: lambda c: _read_list_body(c, 0),
    COL_MAP: lambda c: _read_map_body(c, 0),
    COL_NODE: lambda c: _read_node_body(c, 0),
    COL_RELATIONSHIP: lambda c: _read_rel_body(c, 0),
    COL_PATH: lambda c: _read_path_body(c, 0),
    COL_BYTES: lambda c: _read_bytes_body(c, 0),
    COL_DATE: _temporal_reader("<q", 8),
    COL_LOCAL_TIME: _temporal_reader("<q", 8),
    COL_LOCAL_DATE_TIME: _temporal_reader("<qi", 12),
    COL_ZONED_DATE_TIME: _temporal_reader("<qii", 16),
    COL_DURATION: _temporal_reader("<qq", 16),
    COL_POINT_2D: _temporal_reader("<idd", 20),
    COL_POINT_3D: _temporal_reader("<iddd", 28),
}


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
