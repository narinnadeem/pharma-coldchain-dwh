-- Created automatically by scripts/collect_metrics.py; kept here for documentation.
CREATE TABLE IF NOT EXISTS staging.metrics_storage (snapshot_ts TIMESTAMP NOT NULL, layer TEXT NOT NULL, table_name TEXT NOT NULL, row_count BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS staging.metrics_footprint (snapshot_ts TIMESTAMP NOT NULL, component TEXT NOT NULL, size_mb NUMERIC(12,2) NOT NULL);
CREATE TABLE IF NOT EXISTS staging.metrics_date_coverage (snapshot_ts TIMESTAMP NOT NULL, layer TEXT NOT NULL, table_name TEXT NOT NULL, date_column TEXT, min_date DATE, max_date DATE, span_days INT, year_months INT);
CREATE TABLE IF NOT EXISTS staging.metrics_query_log (log_id INT, query_name TEXT, execution_time_ms DOUBLE PRECISION, rows_returned BIGINT, executed_at TIMESTAMP, collected_at TIMESTAMP DEFAULT now(), CONSTRAINT uq_metrics_query_log UNIQUE (log_id, query_name, executed_at));
