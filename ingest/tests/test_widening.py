from app.store import (
    TYPE_RANK,
    _alter_using,
    _infer_field_type,
    _widened_type,
)


def test_type_rank_orders_lattice():
    assert (
        TYPE_RANK["BOOLEAN"]
        < TYPE_RANK["BIGINT"]
        < TYPE_RANK["DOUBLE PRECISION"]
        < TYPE_RANK["TEXT"]
    )


def test_widened_type_no_change_when_value_fits():
    # observed value already representable by the current column type
    assert _widened_type("BIGINT", 5) == "BIGINT"
    assert _widened_type("DOUBLE PRECISION", 5) == "DOUBLE PRECISION"
    assert _widened_type("DOUBLE PRECISION", 5.5) == "DOUBLE PRECISION"
    assert _widened_type("TEXT", "anything") == "TEXT"
    assert _widened_type("TEXT", 5) == "TEXT"
    assert _widened_type("BOOLEAN", True) == "BOOLEAN"


def test_widened_type_promotes_up_lattice():
    assert _widened_type("BOOLEAN", 5) == "BIGINT"
    assert _widened_type("BOOLEAN", 5.5) == "DOUBLE PRECISION"
    assert _widened_type("BIGINT", 5.5) == "DOUBLE PRECISION"


def test_widened_type_falls_back_to_text_for_strings():
    assert _widened_type("BIGINT", "hello") == "TEXT"
    assert _widened_type("DOUBLE PRECISION", "hello") == "TEXT"
    assert _widened_type("BOOLEAN", "hello") == "TEXT"


def test_widening_is_monotonic_per_value_sequence():
    # Simulate a sequence of observed values; column type should never demote.
    col = _infer_field_type(True)  # starts BOOLEAN
    for v in [True, False, 1, 2, 3.5, "now-a-string", 0]:
        col = _widened_type(col, v)
    assert col == "TEXT"


def test_alter_using_bool_to_numeric_chains_through_int():
    assert _alter_using("f_v", "BOOLEAN", "BIGINT") == '("f_v")::int::bigint'
    assert _alter_using("f_v", "BOOLEAN", "DOUBLE PRECISION") == '("f_v")::int::double precision'


def test_alter_using_simple_casts():
    assert _alter_using("f_v", "BIGINT", "DOUBLE PRECISION") == '("f_v")::double precision'
    assert _alter_using("f_v", "BIGINT", "TEXT") == '("f_v")::text'
    assert _alter_using("f_v", "DOUBLE PRECISION", "TEXT") == '("f_v")::text'
    assert _alter_using("f_v", "BOOLEAN", "TEXT") == '("f_v")::text'


def test_alter_using_quotes_identifier():
    # column names with weird chars get fully quoted
    assert _alter_using('weird"col', "BIGINT", "TEXT") == '("weird""col")::text'
