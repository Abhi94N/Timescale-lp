from app import engine
from app.config import settings


def test_s3_secret_sql_contains_endpoint_and_keys():
    sql = engine.s3_secret_sql()
    assert "TYPE S3" in sql
    assert settings.s3_endpoint_host in sql
    assert "USE_SSL false" in sql
    assert "URL_STYLE 'path'" in sql


def test_attach_pg_sql_read_only():
    sql = engine.attach_pg_sql()
    assert sql.startswith("ATTACH IF NOT EXISTS")
    assert "TYPE postgres" in sql
    assert "READ_ONLY" in sql
    assert settings.tsdb_database in sql


def test_read_parquet_ref_union_by_name():
    ref = engine.read_parquet_ref(["s3://b/a.parquet", "s3://b/c.parquet"])
    assert ref.startswith("read_parquet([")
    assert "union_by_name=true" in ref
    assert "'s3://b/a.parquet'" in ref


def test_build_federated_view_sql_dedups_by_seq():
    sql = engine.build_federated_view_sql("lp", "cpu", ["pg.lp.cpu", "read_parquet(['x'])"])
    assert 'CREATE OR REPLACE VIEW "lp"."cpu"' in sql
    assert "UNION ALL BY NAME" in sql
    assert "row_number() OVER (PARTITION BY time, tag_hash ORDER BY seq DESC)" in sql
    assert "EXCLUDE (seq, _rn)" in sql
    assert "_rn = 1" in sql


def test_build_federated_view_sql_empty():
    sql = engine.build_federated_view_sql("lp", "cpu", [])
    assert "WHERE false" in sql


def test_compact_sql_dedups_and_keeps_seq():
    sql = engine.compact_sql(["s3://b/a.parquet"], "s3://b/cold/x.parquet")
    assert sql.startswith("COPY (")
    assert "row_number() OVER" in sql
    assert "FORMAT parquet" in sql
    # seq is NOT excluded in compaction output (needed for cross-tier dedup)
    assert "EXCLUDE (seq" not in sql


def test_duckdb_dedup_view_keeps_newest_seq(tmp_path):
    """End-to-end check of the federation dedup semantic against a real DuckDB:
    a newer (higher-seq) version in 'hot' must win over an older 'cold' row for
    the same (time, tag_hash)."""
    import duckdb

    con = duckdb.connect(":memory:")
    # Stand-ins for hot (PG) and cold (Parquet) — both carry time, tag_hash, seq.
    con.execute("CREATE TABLE hot (time INTEGER, tag_hash BIGINT, seq BIGINT, f_value DOUBLE)")
    con.execute("INSERT INTO hot VALUES (1, 100, 50, 9.9)")  # updated value, high seq
    con.execute("CREATE TABLE cold (time INTEGER, tag_hash BIGINT, seq BIGINT, f_value DOUBLE)")
    con.execute("INSERT INTO cold VALUES (1, 100, 10, 1.1)")  # original, low seq
    con.execute("INSERT INTO cold VALUES (2, 100, 20, 2.2)")  # only in cold

    view_sql = engine.build_federated_view_sql("main", "cpu", ["hot", "cold"])
    con.execute(view_sql)
    rows = con.execute('SELECT time, f_value FROM "main"."cpu" ORDER BY time').fetchall()
    assert rows == [(1, 9.9), (2, 2.2)]  # dedup kept hot's newer value for time=1
    # seq is not exposed by the view
    cols = [d[0] for d in con.execute('SELECT * FROM "main"."cpu"').description]
    assert "seq" not in cols
    con.close()
