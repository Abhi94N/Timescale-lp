from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    tsdb_host: str = "localhost"
    tsdb_port: int = 5432
    tsdb_user: str = "tsdbadmin"
    tsdb_password: str = "tsdbpass"
    tsdb_database: str = "metrics"
    tsdb_schema: str = "lp"

    ingest_http_addr: str = ":8080"
    ingest_pool_min: int = 4
    ingest_pool_max: int = 32
    ingest_batch_size: int = 5000
    ingest_batch_flush_ms: int = 500
    ingest_workers: int = 8
    ingest_chunk_interval: str = "1 day"
    ingest_compression_after: str = "7 days"

    # --- Tiered storage (hot = TimescaleDB, cold = Parquet on S3/MinIO) ---
    # Data whose chunk is entirely older than this window is eligible to be
    # moved out of TimescaleDB into Parquet objects.
    tier_hot_window: str = "7 days"
    # Background tierer (in-process). Off by default; the manual
    # POST /api/v1/tier/run endpoint works regardless.
    tier_enabled: bool = False
    tier_interval_seconds: int = 3600
    # Enables the DuckDB-backed federated query path (tier=all / tier=cold).
    query_engine_enabled: bool = True

    # S3 / MinIO (S3-compatible) cold object store.
    s3_endpoint: str = "http://minio:9000"
    s3_region: str = "us-east-1"
    s3_bucket: str = "timescale-cold"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    # Path-style addressing + plaintext are the norm for local MinIO.
    s3_url_style: str = "path"

    @property
    def s3_use_ssl(self) -> bool:
        return self.s3_endpoint.lower().startswith("https://")

    @property
    def s3_endpoint_host(self) -> str:
        """Endpoint without scheme — the form DuckDB's S3 secret expects."""
        ep = self.s3_endpoint
        for prefix in ("https://", "http://"):
            if ep.startswith(prefix):
                return ep[len(prefix) :]
        return ep

    @property
    def duckdb_pg_dsn(self) -> str:
        """libpq keyword/value DSN for DuckDB's postgres ATTACH."""
        return (
            f"host={self.tsdb_host} port={self.tsdb_port} "
            f"dbname={self.tsdb_database} user={self.tsdb_user} "
            f"password={self.tsdb_password}"
        )

    @property
    def dsn(self) -> str:
        return (
            f"postgres://{self.tsdb_user}:{self.tsdb_password}"
            f"@{self.tsdb_host}:{self.tsdb_port}/{self.tsdb_database}"
        )

    @property
    def http_host(self) -> str:
        addr = self.ingest_http_addr
        if addr.startswith(":"):
            return "0.0.0.0"
        return addr.split(":")[0] or "0.0.0.0"

    @property
    def http_port(self) -> int:
        addr = self.ingest_http_addr
        return int(addr.rsplit(":", 1)[-1])


settings = Settings()
