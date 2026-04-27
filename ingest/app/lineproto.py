"""
InfluxDB line protocol parser.

Format:
    measurement[,tag_key=tag_val...] field_key=field_val[,field_key=field_val...] [timestamp]

Escaping:
    - measurement: comma and space must be backslash-escaped
    - tag keys/values + field keys: comma, equals, and space must be backslash-escaped
    - string field values: enclosed in double quotes, internal `"` and `\\` are backslash-escaped
    - field values are typed:
        12i           -> integer
        12u           -> unsigned int (treated as integer)
        12.5          -> float
        12            -> float (Influx default for unsuffixed numerics)
        t/T/true/...  -> boolean true
        f/F/false/... -> boolean false
        "abc"         -> string
    - timestamp: optional integer (default precision: nanoseconds)

This implementation is forgiving: parse errors on a single line are reported
but do not abort the whole batch.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

PRECISION_TO_NS = {
    "ns": 1,
    "us": 1_000,
    "ms": 1_000_000,
    "s": 1_000_000_000,
}


class LineProtocolError(ValueError):
    pass


@dataclass(slots=True)
class Point:
    measurement: str
    tags: dict[str, str] = field(default_factory=dict)
    fields: dict[str, object] = field(default_factory=dict)
    # Stored as a UTC datetime for asyncpg TIMESTAMPTZ binding.
    time: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


def _unescape(s: str, delims: set[str]) -> str:
    out: list[str] = []
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == "\\" and i + 1 < n and s[i + 1] in delims | {"\\"}:
            out.append(s[i + 1])
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _split_top(s: str, sep: str, respect_quotes: bool = False) -> list[str]:
    """Split on `sep` ignoring backslash-escaped separators and (optionally) quoted regions."""
    parts: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(s)
    in_quotes = False
    while i < n:
        c = s[i]
        if respect_quotes and c == '"' and (i == 0 or s[i - 1] != "\\"):
            in_quotes = not in_quotes
            buf.append(c)
            i += 1
            continue
        if c == "\\" and i + 1 < n:
            buf.append(c)
            buf.append(s[i + 1])
            i += 2
            continue
        if c == sep and not in_quotes:
            parts.append("".join(buf))
            buf.clear()
            i += 1
            continue
        buf.append(c)
        i += 1
    parts.append("".join(buf))
    return parts


def _split_top_space(s: str) -> list[str]:
    """Split into the (measurement+tags) / fields / [timestamp] sections on unescaped spaces, ignoring spaces inside quoted strings."""
    parts: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(s)
    in_quotes = False
    while i < n:
        c = s[i]
        if c == '"' and (i == 0 or s[i - 1] != "\\"):
            in_quotes = not in_quotes
            buf.append(c)
            i += 1
            continue
        if c == "\\" and i + 1 < n:
            buf.append(c)
            buf.append(s[i + 1])
            i += 2
            continue
        if c == " " and not in_quotes:
            parts.append("".join(buf))
            buf.clear()
            i += 1
            continue
        buf.append(c)
        i += 1
    parts.append("".join(buf))
    return parts


def _parse_field_value(raw: str) -> object:
    if not raw:
        raise LineProtocolError("empty field value")
    if raw[0] == '"' and raw[-1] == '"' and len(raw) >= 2:
        inner = raw[1:-1]
        out: list[str] = []
        i = 0
        n = len(inner)
        while i < n:
            c = inner[i]
            if c == "\\" and i + 1 < n and inner[i + 1] in ('"', "\\"):
                out.append(inner[i + 1])
                i += 2
                continue
            out.append(c)
            i += 1
        return "".join(out)
    low = raw.lower()
    if low in ("t", "true"):
        return True
    if low in ("f", "false"):
        return False
    last = raw[-1]
    if last == "i":
        return int(raw[:-1])
    if last == "u":
        return int(raw[:-1])
    try:
        return float(raw)
    except ValueError as exc:
        raise LineProtocolError(f"invalid field value: {raw!r}") from exc


def parse_line(line: str, *, precision: str = "ns", default_ts_ns: int | None = None) -> Point:
    line = line.strip()
    if not line or line.startswith("#"):
        raise LineProtocolError("empty or comment line")

    sections = _split_top_space(line)
    if len(sections) < 2:
        raise LineProtocolError("line protocol requires measurement and at least one field")

    head = sections[0]
    fields_section = sections[1]
    ts_section = sections[2] if len(sections) >= 3 else None

    head_parts = _split_top(head, ",", respect_quotes=False)
    measurement = _unescape(head_parts[0], {",", " "})
    if not measurement:
        raise LineProtocolError("missing measurement")

    tags: dict[str, str] = {}
    for tp in head_parts[1:]:
        if not tp:
            continue
        kv = _split_top(tp, "=", respect_quotes=False)
        if len(kv) != 2:
            raise LineProtocolError(f"invalid tag pair: {tp!r}")
        k = _unescape(kv[0], {",", "=", " "})
        v = _unescape(kv[1], {",", "=", " "})
        if not k:
            raise LineProtocolError("empty tag key")
        tags[k] = v

    fields: dict[str, object] = {}
    for fp in _split_top(fields_section, ",", respect_quotes=True):
        if not fp:
            continue
        kv = _split_top(fp, "=", respect_quotes=True)
        if len(kv) != 2:
            raise LineProtocolError(f"invalid field pair: {fp!r}")
        k = _unescape(kv[0], {",", "=", " "})
        if not k:
            raise LineProtocolError("empty field key")
        fields[k] = _parse_field_value(kv[1])
    if not fields:
        raise LineProtocolError("at least one field required")

    if ts_section:
        try:
            ts_raw = int(ts_section)
        except ValueError as exc:
            raise LineProtocolError(f"invalid timestamp: {ts_section!r}") from exc
        ns = ts_raw * PRECISION_TO_NS[precision]
    elif default_ts_ns is not None:
        ns = default_ts_ns
    else:
        ns = None

    ts_dt = (
        datetime.now(tz=UTC) if ns is None else datetime.fromtimestamp(ns / 1_000_000_000, tz=UTC)
    )

    return Point(measurement=measurement, tags=tags, fields=fields, time=ts_dt)


def parse_batch(body: str, *, precision: str = "ns") -> tuple[list[Point], list[tuple[int, str]]]:
    """Parse a multi-line line-protocol payload. Returns (points, errors)."""
    points: list[Point] = []
    errors: list[tuple[int, str]] = []
    for idx, raw in enumerate(body.splitlines()):
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        try:
            points.append(parse_line(s, precision=precision))
        except LineProtocolError as exc:
            errors.append((idx + 1, str(exc)))
    return points, errors
