from datetime import UTC

import pytest

from app.lineproto import LineProtocolError, parse_batch, parse_line


def test_parse_minimal():
    p = parse_line("cpu value=42")
    assert p.measurement == "cpu"
    assert p.tags == {}
    assert p.fields == {"value": 42.0}


def test_parse_full():
    p = parse_line("cpu,host=a,region=us value=1.5,count=3i,ok=true 1700000000000000000")
    assert p.measurement == "cpu"
    assert p.tags == {"host": "a", "region": "us"}
    assert p.fields == {"value": 1.5, "count": 3, "ok": True}
    assert p.time.tzinfo is UTC
    assert int(p.time.timestamp()) == 1700000000


def test_parse_string_field_with_escapes():
    p = parse_line(r'log msg="hello \"world\"" 1')
    assert p.fields == {"msg": 'hello "world"'}


def test_parse_escaped_measurement_and_tag():
    p = parse_line(r"weird\ name,host=a\,b value=1")
    assert p.measurement == "weird name"
    assert p.tags == {"host": "a,b"}


def test_parse_unsigned_and_int():
    p = parse_line("m a=10i,b=20u")
    assert p.fields == {"a": 10, "b": 20}


def test_parse_precision_ms():
    p = parse_line("m v=1 1700000000000", precision="ms")
    assert int(p.time.timestamp()) == 1700000000


def test_parse_invalid_no_field():
    with pytest.raises(LineProtocolError):
        parse_line("cpu,host=a")


def test_parse_batch_collects_errors():
    body = "good v=1\nbad line here\n# comment\n\nalso v=2 1\n"
    points, errors = parse_batch(body)
    assert len(points) == 2
    assert len(errors) == 1
    assert errors[0][0] == 2


def test_parse_quoted_field_with_comma_and_space():
    p = parse_line('m,k=v label="hello, world",n=1i')
    assert p.fields == {"label": "hello, world", "n": 1}
