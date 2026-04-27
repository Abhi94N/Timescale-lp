from app.store import (
    _coerce,
    _field_col,
    _hash_tags,
    _infer_field_type,
    _measurement_table,
    _safe_ident,
    _tag_col,
)


def test_safe_ident_lowercases_and_substitutes():
    assert _safe_ident("Cpu Usage!") == "cpu_usage_"
    assert _safe_ident("123abc").startswith("_")
    long = "a" * 100
    assert len(_safe_ident(long)) == 63


def test_column_name_helpers():
    assert _measurement_table("CPU/Load") == "cpu_load"
    assert _tag_col("Host") == "t_host"
    assert _field_col("Value-1") == "f_value_1"


def test_infer_field_type():
    assert _infer_field_type(True) == "BOOLEAN"
    assert _infer_field_type(1) == "BIGINT"
    assert _infer_field_type(1.5) == "DOUBLE PRECISION"
    assert _infer_field_type("s") == "TEXT"


def test_hash_tags_is_stable_and_order_invariant():
    h1 = _hash_tags({"a": "1", "b": "2"})
    h2 = _hash_tags({"b": "2", "a": "1"})
    assert h1 == h2
    assert _hash_tags({}) == 0
    assert h1 != _hash_tags({"a": "1", "b": "3"})
    # fits in signed BIGINT
    assert 0 <= h1 < 2**63


def test_coerce_types():
    assert _coerce(None, "BIGINT") is None
    assert _coerce(True, "BIGINT") == 1
    assert _coerce("3", "BIGINT") == 3
    assert _coerce(3, "DOUBLE PRECISION") == 3.0
    assert _coerce("true", "BOOLEAN") is True
    assert _coerce(0, "BOOLEAN") is False
    assert _coerce(123, "TEXT") == "123"
