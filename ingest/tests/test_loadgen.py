import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import loadgen


def test_build_batch_row_count_and_format():
    payload, max_ts = loadgen.build_batch("load", 0, 3, series=2, base_ts_ns=0, step_ns=10)
    lines = payload.splitlines()
    assert len(lines) == 3
    # measurement + tags + fields + timestamp
    assert lines[0].startswith("load,host=h0,region=r0 ")
    assert lines[1].startswith("load,host=h1,region=r1 ")
    assert lines[2].startswith("load,host=h0,region=r0 ")  # series wraps at 2
    assert lines[2].endswith(" 20")  # ts = base + i*step = 2*10
    assert max_ts == 20


def test_build_batch_is_parseable_line_protocol():
    from app.lineproto import parse_batch

    payload, _ = loadgen.build_batch("load", 100, 50, series=10, base_ts_ns=1_000, step_ns=5)
    points, errors = parse_batch(payload)
    assert errors == []
    assert len(points) == 50
    assert points[0].measurement == "load"
    assert "host" in points[0].tags
    assert "value" in points[0].fields and "count" in points[0].fields


def test_parse_args_duration_overrides_rows():
    args = loadgen.parse_args(["--duration", "30"])
    assert args.duration == 30
    args2 = loadgen.parse_args([])
    assert args2.duration is None
