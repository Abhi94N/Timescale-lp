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
