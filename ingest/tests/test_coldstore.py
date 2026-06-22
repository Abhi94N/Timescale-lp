from datetime import UTC, datetime

from app import coldstore


def test_staging_key_zero_padded_sortable():
    k = coldstore.staging_key("cpu", 5, 9)
    assert k == "cpu/staging/00000000000000000005-00000000000000000009.parquet"


def test_cold_key_layout_is_hive_partitioned():
    k = coldstore.cold_key("cpu", "2026-06-01", 42)
    assert k == "cpu/cold/dt=2026-06-01/00000000000000000042.parquet"


def test_day_of_is_utc():
    ts = datetime(2026, 6, 1, 23, 30, tzinfo=UTC)
    assert coldstore.day_of(ts) == "2026-06-01"


def _obj(s, e):
    return {
        "range_start": datetime(2026, 6, s, tzinfo=UTC),
        "range_end": datetime(2026, 6, e, tzinfo=UTC),
    }


def test_select_overlapping_prunes_by_window():
    objs = [_obj(1, 2), _obj(5, 6), _obj(9, 10)]
    # window [4, 7] overlaps only the middle object
    got = coldstore.select_overlapping(
        objs, datetime(2026, 6, 4, tzinfo=UTC), datetime(2026, 6, 7, tzinfo=UTC)
    )
    assert got == [_obj(5, 6)]


def test_select_overlapping_open_bounds():
    objs = [_obj(1, 2), _obj(5, 6)]
    # no bounds -> everything
    assert coldstore.select_overlapping(objs, None, None) == objs
    # only start -> drop objects ending before it
    got = coldstore.select_overlapping(objs, datetime(2026, 6, 4, tzinfo=UTC), None)
    assert got == [_obj(5, 6)]
