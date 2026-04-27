CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE SCHEMA IF NOT EXISTS lp;

COMMENT ON SCHEMA lp IS 'Line-protocol-managed measurements (one hypertable per measurement)';
