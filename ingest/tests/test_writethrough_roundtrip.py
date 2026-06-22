"""
End-to-end write-through + federation on a LOCAL filesystem (no Docker).

Exercises the real code: build the typed Arrow table from row tuples, write a
staging Parquet via ColdStore's pyarrow filesystem, then read it back through
DuckDB and the dedup federation view. This is the same code that runs against
S3/MinIO in production — only the filesystem differs.
"""

from datetime import UTC, datetime

from app import engine
from app.coldstore import ColdStore, staging_key
from app.store import TableSchema, build_arrow_table


def _schema() -> TableSchema:
    return TableSchema(
        measurement="cpu",
        table="cpu",
        qualified='"lp"."cpu"',
        tag_cols={"host": "t_host"},
        field_cols={"value": ("f_value", "DOUBLE PRECISION")},
    )


def _rows():
    cols = ["time", "tag_hash", "seq", "t_host", "f_value"]
    t = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    records = [
        (t, 100, 10, "a", 1.1),  # original
        (t, 100, 20, "a", 9.9),  # update (same time+tag_hash, higher seq)
        (t, 200, 11, "b", 2.2),  # different series
    ]
    return cols, records


def test_staging_roundtrip_and_dedup(tmp_path):
    import duckdb

    cols, records = _rows()
    schema = _schema()
    table = build_arrow_table(cols, records, schema)

    # Sanity: arrow types are as declared.
    assert table.num_rows == 3
    assert table.schema.field("time").type.tz is not None
    assert str(table.schema.field("f_value").type) == "double"
    assert str(table.schema.field("seq").type) == "int64"

    cold = ColdStore.local(store=None, root=str(tmp_path / "bucket"))
    key = staging_key("cpu", 10, 20)
    nbytes = cold.write_table(key, table)
    assert nbytes > 0

    con = duckdb.connect(":memory:")
    uri = cold.uri(key)

    # Raw read: all three rows present, seq retained.
    raw = con.execute(f"SELECT count(*) FROM read_parquet('{uri}')").fetchone()[0]
    assert raw == 3

    # Federated dedup view over just this cold object: the (time,200... wait
    # the duplicate (time,100) collapses to the highest-seq value.
    ref = engine.read_parquet_ref([uri])
    con.execute(engine.build_federated_view_sql("main", "cpu", [ref]))
    out = con.execute('SELECT tag_hash, f_value FROM "main"."cpu" ORDER BY tag_hash').fetchall()
    assert out == [(100, 9.9), (200, 2.2)]  # update won; second series intact
    con.close()
