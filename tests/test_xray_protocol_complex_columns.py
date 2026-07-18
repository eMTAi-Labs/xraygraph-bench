"""#27 — complex column decode (NODE / RELATIONSHIP / PATH / LIST / MAP + nulls).

RED on the pre-fix client: NODE/REL/PATH columns hit the silent-None
unknown-type skip, so `_decode_batch` returned `{"n": None}` for every row —
LDBC IS/IC queries that RETURN nodes/paths lost their values entirely.
GREEN post-fix: the columns decode to dicts, a null row stays None (via the
zero-length-cell guard, not a crash), and — the desync guard — a scalar
column following a complex column still decodes, proving offset advancement
stays exactly `data_start + data_length`.

The synthetic column bytes here are built independently, byte-for-byte
against the engine encoder widths verified in the adversarial preflight
(WriteI64 signed i64 ids; WriteString u16 label/key lens; u32 counts/offsets;
WriteLongString u32 recursive strings; i32 signed PATH steps; AlignTo8
payload-relative envelope). A live golden-fixture capture from .187 is the
final authoritative check (see test_live_golden, skipped without XG_LIVE).
"""

from __future__ import annotations

import math
import os
import struct

import pytest

from tools.xraybench.adapters import xray_protocol as xp
from tools.xraybench.adapters.xray_protocol import XrayProtocolClient, XrayProtocolError


# ── synthetic wire builders (independent of the decoder) ────────────────────


def _u16s(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack("<H", len(b)) + b


def _longstr(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack("<I", len(b)) + b


def _val_int(v: int) -> bytes:
    return struct.pack("<Bq", 0x03, v)


def _val_str(v: str) -> bytes:
    return struct.pack("<B", 0x05) + _longstr(v)


def _node_body(gid: int, labels: list[str], props: list[tuple[str, bytes]]) -> bytes:
    b = struct.pack("<q", gid)
    b += struct.pack("<I", len(labels))
    for lb in labels:
        b += _u16s(lb)
    b += struct.pack("<I", len(props))
    for k, tagged_v in props:
        b += _u16s(k) + tagged_v
    return b


def _rel_body(
    rid: int, start: int, end: int, rtype: str, props: list[tuple[str, bytes]]
) -> bytes:
    b = struct.pack("<qqq", rid, start, end)
    b += _u16s(rtype)
    b += struct.pack("<I", len(props))
    for k, tagged_v in props:
        b += _u16s(k) + tagged_v
    return b


def _list_body(elems: list[bytes]) -> bytes:
    return struct.pack("<I", len(elems)) + b"".join(elems)


def _map_body(entries: list[tuple[str, bytes]]) -> bytes:
    b = struct.pack("<I", len(entries))
    for k, tagged_v in entries:
        b += _u16s(k) + tagged_v
    return b


def _path_body(nodes: list[bytes], rels: list[bytes], seq: list[int]) -> bytes:
    b = struct.pack("<I", len(nodes)) + b"".join(nodes)
    b += struct.pack("<I", len(rels)) + b"".join(rels)
    b += struct.pack("<I", len(seq))
    for s in seq:
        b += struct.pack("<i", s)
    return b


def _offsetblob_body(cells: list[bytes | None]) -> tuple[bytes, bytes]:
    """Build (body, bitmap) for a variable-width column.

    body = u32 blob_size; u32 offsets[N+1]; blob. A None cell is zero-length
    (offsets[r] == offsets[r+1]) and null in the bitmap.
    """
    row_count = len(cells)
    blob = b"".join(c or b"" for c in cells)
    offsets = [0]
    acc = 0
    for c in cells:
        acc += len(c or b"")
        offsets.append(acc)
    body = struct.pack("<I", len(blob))
    for o in offsets:
        body += struct.pack("<I", o)
    body += blob

    bitmap_len = math.ceil(row_count / 8)
    bitmap = bytearray(b"\x00" * bitmap_len)
    for r, c in enumerate(cells):
        if c is not None:
            bitmap[r // 8] |= 1 << (r % 8)
    return (body, bytes(bitmap))


def _int64_body(vals: list[int]) -> tuple[bytes, bytes]:
    row_count = len(vals)
    body = b"".join(struct.pack("<q", v) for v in vals)
    bitmap = b"\xff" * math.ceil(row_count / 8)
    return (body, bitmap)


def _frame_column(payload: bytearray, body: bytes, bitmap: bytes) -> None:
    """Append one column exactly as the engine EncodeBatch does:
    u32 data_length; AlignTo8 (pad the payload up to 8); body; null bitmap.
    data_length is the body length measured from the post-pad data_start."""
    payload += struct.pack("<I", len(body))
    while len(payload) % 8 != 0:
        payload += b"\x00"
    payload += body
    payload += bitmap


def _batch_payload(row_count: int, columns: list[tuple[bytes, bytes]]) -> bytes:
    """[u32 row_count][u16 col_count][u32 sv_len=0] then each framed column."""
    payload = bytearray()
    payload += struct.pack("<I", row_count)
    payload += struct.pack("<H", len(columns))
    payload += struct.pack("<I", 0)  # no selection vector
    for body, bitmap in columns:
        _frame_column(payload, body, bitmap)
    return bytes(payload)


def _client() -> XrayProtocolClient:
    return XrayProtocolClient("127.0.0.1")


# ── tests ───────────────────────────────────────────────────────────────────


def test_node_column_decodes():
    node = _node_body(
        42, ["Person"], [("name", _val_str("alice")), ("age", _val_int(30))]
    )
    payload = _batch_payload(1, [_offsetblob_body([node])])
    rows = _client()._decode_batch(payload, [("n", xp.COL_NODE)])
    assert rows == [
        {"n": {"id": 42, "labels": ["Person"], "properties": {"name": "alice", "age": 30}}}
    ]


def test_relationship_column_decodes():
    rel = _rel_body(7, 42, 43, "KNOWS", [("since", _val_int(2020))])
    payload = _batch_payload(1, [_offsetblob_body([rel])])
    rows = _client()._decode_batch(payload, [("r", xp.COL_RELATIONSHIP)])
    assert rows == [
        {
            "r": {
                "id": 7,
                "start": 42,
                "end": 43,
                "type": "KNOWS",
                "properties": {"since": 2020},
            }
        }
    ]


def test_path_column_decodes_with_signed_sequence():
    n0 = _node_body(1, ["A"], [])
    n1 = _node_body(2, ["B"], [])
    r0 = _rel_body(10, 1, 2, "E", [])
    path = _path_body([n0, n1], [r0], [1])  # forward: rels[0]
    payload = _batch_payload(1, [_offsetblob_body([path])])
    rows = _client()._decode_batch(payload, [("p", xp.COL_PATH)])
    p = rows[0]["p"]
    assert [n["id"] for n in p["nodes"]] == [1, 2]
    assert p["relationships"][0]["id"] == 10
    assert p["sequence"] == [1]


def test_list_and_map_columns_decode():
    lst = _list_body([_val_int(1), _val_int(2), _val_int(3)])
    payload = _batch_payload(1, [_offsetblob_body([lst])])
    rows = _client()._decode_batch(payload, [("xs", xp.COL_LIST)])
    assert rows == [{"xs": [1, 2, 3]}]

    mp = _map_body([("k", _val_str("v")), ("n", _val_int(9))])
    payload = _batch_payload(1, [_offsetblob_body([mp])])
    rows = _client()._decode_batch(payload, [("m", xp.COL_MAP)])
    assert rows == [{"m": {"k": "v", "n": 9}}]


def test_null_node_cell_is_none_not_crash():
    node = _node_body(1, ["X"], [])
    # row 0 = node, row 1 = null (zero-length cell)
    payload = _batch_payload(2, [_offsetblob_body([node, None])])
    rows = _client()._decode_batch(payload, [("n", xp.COL_NODE)])
    assert rows[0]["n"]["id"] == 1
    assert rows[1]["n"] is None


def test_node_column_followed_by_int_column_no_desync():
    """The desync guard: after decoding a variable-width complex column, the
    reader must land exactly on data_start+data_length so the next column
    still decodes. Pre-fix this held (skip), but the decode path must preserve
    it now that we actually parse the body."""
    node = _node_body(99, ["Z"], [("k", _val_int(5))])
    payload = _batch_payload(
        1, [_offsetblob_body([node]), _int64_body([12345])]
    )
    rows = _client()._decode_batch(
        payload, [("n", xp.COL_NODE), ("cnt", xp.COL_INT64)]
    )
    assert rows[0]["n"]["id"] == 99
    assert rows[0]["cnt"] == 12345


def test_unknown_column_type_raises_not_silent_none():
    # A capability-gated / future column type must fail LOUDLY in a
    # correctness harness — never silently decode to None.
    payload = _batch_payload(1, [_offsetblob_body([b"\x00\x00"])])
    with pytest.raises(XrayProtocolError):
        _client()._decode_batch(payload, [("x", 0x13)])  # LIST_INT64 (CAP_TYPED_NESTED)


@pytest.mark.skipif(
    not os.environ.get("XG_LIVE"),
    reason="live golden capture requires XG_LIVE=host:port:user:passfile:db",
)
def test_live_golden_return_node():
    """Authoritative falsifier: CREATE a node on a live engine and MATCH ...
    RETURN n, asserting the decoded node round-trips. Confirms the real
    column tag (0x08 NODE vs 0x05 stringified) for the running executor."""
    host, port, user, passfile, db = os.environ["XG_LIVE"].split(":")
    pw = open(passfile).read().strip()
    c = XrayProtocolClient(host, int(port))
    c.connect(user=user, password=pw, database=db)
    try:
        c.execute("CREATE (:BenchNode {name: 'gold', n: 7})")
        rows = c.execute("MATCH (x:BenchNode) RETURN x")
        assert rows, "no rows"
        node = rows[0]["x"]
        assert isinstance(node, dict) and "id" in node, f"got {node!r}"
        assert node["properties"].get("name") == "gold"
    finally:
        try:
            c.execute("MATCH (x:BenchNode) DETACH DELETE x")
        finally:
            c.close()
