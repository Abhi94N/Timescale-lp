from datetime import UTC, datetime

from app import engine
from app.config import settings


def test_cold_key_is_utc_deterministic():
    ts = datetime(2026, 6, 1, 5, 30, 0, tzinfo=UTC)
    assert engine.cold_key("cpu", ts) == "cpu/20260601T053000Z.parquet"


def test_cold_key_normalizes_offset_to_utc():
    from datetime import timedelta, timezone

    ts = datetime(2026, 6, 1, 7, 30, 0, tzinfo=timezone(timedelta(hours=2)))
    # 07:30+02:00 == 05:30Z
    assert engine.cold_key("cpu", ts) == "cpu/20260601T053000Z.parquet"


def test_s3_uri_uses_bucket():
    assert engine.s3_uri("cpu/x.parquet") == f"s3://{settings.s3_bucket}/cpu/x.parquet"


def test_s3_secret_sql_contains_endpoint_and_keys():
    sql = engine.s3_secret_sql()
    assert "TYPE S3" in sql
    assert settings.s3_endpoint_host in sql
    assert "USE_SSL false" in sql  # default http endpoint
    assert "URL_STYLE 'path'" in sql


def test_attach_pg_sql_read_only():
    sql = engine.attach_pg_sql()
    assert sql.startswith("ATTACH IF NOT EXISTS")
    assert "TYPE postgres" in sql
    assert "READ_ONLY" in sql
    assert settings.tsdb_database in sql


def test_copy_to_parquet_sql():
    sql = engine.copy_to_parquet_sql("SELECT 1", "cpu/x.parquet")
    assert sql.startswith("COPY (SELECT 1) TO 's3://")
    assert "FORMAT parquet" in sql
    assert "OVERWRITE_OR_IGNORE true" in sql


def test_build_view_sql_hot_only():
    sql = engine.build_view_sql("lp", "cpu", "pg.lp.cpu", [])
    assert sql == 'CREATE OR REPLACE VIEW "lp"."cpu" AS SELECT * FROM pg.lp.cpu'


def test_build_view_sql_cold_only():
    sql = engine.build_view_sql("lp", "cpu", None, ["cpu/a.parquet", "cpu/b.parquet"])
    assert "read_parquet([" in sql
    assert "union_by_name=true" in sql
    assert "pg.lp.cpu" not in sql


def test_build_view_sql_hot_and_cold_uses_union_by_name():
    sql = engine.build_view_sql("lp", "cpu", "pg.lp.cpu", ["cpu/a.parquet"])
    assert "SELECT * FROM pg.lp.cpu" in sql
    assert "UNION ALL BY NAME" in sql
    assert "read_parquet([" in sql


def test_build_view_sql_empty_is_degenerate():
    sql = engine.build_view_sql("lp", "cpu", None, [])
    assert "WHERE false" in sql


def test_duckdb_union_by_name_fills_missing_columns():
    """The federation view relies on UNION ALL BY NAME aligning differing
    column sets and filling gaps with NULL. Validate that semantic against a
    real DuckDB so a version change can't silently break federation."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE hot (time INTEGER, f_value DOUBLE, f_new INTEGER)")
    con.execute("INSERT INTO hot VALUES (3, 3.5, 9)")
    con.execute("CREATE TABLE cold (time INTEGER, f_value DOUBLE)")  # no f_new
    con.execute("INSERT INTO cold VALUES (1, 1.5)")

    rows = con.execute(
        "SELECT time, f_value, f_new FROM ("
        "  SELECT * FROM hot UNION ALL BY NAME SELECT * FROM cold"
        ") ORDER BY time"
    ).fetchall()
    assert rows[0] == (1, 1.5, None)  # cold row: f_new filled with NULL
    assert rows[1] == (3, 3.5, 9)
    con.close()
