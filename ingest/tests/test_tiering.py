from datetime import UTC, datetime

from app import tiering


def _dt(d, h=0):
    return datetime(2026, 6, d, h, tzinfo=UTC)


def test_plan_compaction_groups_by_day():
    objs = [
        {"range_start": _dt(1, 1), "min_seq": 1},
        {"range_start": _dt(1, 5), "min_seq": 2},
        {"range_start": _dt(2, 3), "min_seq": 3},
    ]
    groups = tiering.plan_compaction(objs)
    assert set(groups) == {"2026-06-01", "2026-06-02"}
    assert len(groups["2026-06-01"]) == 2
    assert len(groups["2026-06-02"]) == 1


def test_merge_intervals():
    ivs = [(_dt(1), _dt(3)), (_dt(2), _dt(4)), (_dt(6), _dt(7))]
    assert tiering.merge_intervals(ivs) == [(_dt(1), _dt(4)), (_dt(6), _dt(7))]


def test_is_covered_true_when_contiguous():
    ivs = [(_dt(1), _dt(2)), (_dt(2), _dt(4))]
    assert tiering.is_covered(_dt(1), _dt(3), ivs) is True
    assert tiering.is_covered(_dt(1), _dt(4), ivs) is True


def test_is_covered_false_with_gap():
    ivs = [(_dt(1), _dt(2)), (_dt(3), _dt(4))]
    assert tiering.is_covered(_dt(1), _dt(4), ivs) is False  # gap [2,3)


def test_is_covered_false_when_empty():
    assert tiering.is_covered(_dt(1), _dt(2), []) is False
