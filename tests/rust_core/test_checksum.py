import xraybench_core


def test_hash_empty():
    h = xraybench_core.checksum.hash_result_set([])
    assert h.startswith("blake3:")


def test_hash_deterministic():
    rows = [[1, "hello", 3.14], [2, "world", 2.72]]
    h1 = xraybench_core.checksum.hash_result_set(rows)
    h2 = xraybench_core.checksum.hash_result_set(rows)
    assert h1 == h2


def test_hash_order_independent():
    rows_a = [[1, "a"], [2, "b"], [3, "c"]]
    rows_b = [[3, "c"], [1, "a"], [2, "b"]]
    assert xraybench_core.checksum.hash_result_set(rows_a) == \
           xraybench_core.checksum.hash_result_set(rows_b)


def test_hash_different_data():
    assert xraybench_core.checksum.hash_result_set([[1]]) != \
           xraybench_core.checksum.hash_result_set([[2]])


def test_verify_hash():
    rows = [[42, "test"]]
    h = xraybench_core.checksum.hash_result_set(rows)
    assert xraybench_core.checksum.verify_hash(rows, h)
    assert not xraybench_core.checksum.verify_hash(rows, "blake3:" + "0" * 64)


def test_hash_with_none():
    rows = [[1, None, "text"]]
    h = xraybench_core.checksum.hash_result_set(rows)
    assert h.startswith("blake3:")


def test_float_eq_ulp():
    assert xraybench_core.checksum.float_eq_ulp(1.0, 1.0, 0)
    assert not xraybench_core.checksum.float_eq_ulp(1.0, 2.0, 0)
