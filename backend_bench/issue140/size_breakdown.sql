\echo === database
SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size, pg_database_size(current_database()) AS db_bytes;
\echo === row counts
SELECT (SELECT count(*) FROM timeseries) AS ts_rows, (SELECT count(*) FROM ref_ids) AS ref_ids, (SELECT count(*) FROM streams) AS streams;
\echo === hypertable detailed size
SELECT pg_size_pretty(table_bytes) AS heap, pg_size_pretty(index_bytes) AS indexes, pg_size_pretty(toast_bytes) AS toast, pg_size_pretty(total_bytes) AS total, round(100.0*index_bytes/total_bytes,1) AS index_pct FROM hypertable_detailed_size('timeseries');
\echo === per-index size on hypertable
SELECT indexname, pg_size_pretty(hypertable_index_size(indexname::regclass)) AS size, hypertable_index_size(indexname::regclass) AS bytes, indexdef FROM pg_indexes WHERE tablename='timeseries' ORDER BY bytes DESC;
\echo === chunks
SELECT count(*) AS chunks, count(*) FILTER (WHERE is_compressed) AS compressed, min(range_start) AS first, max(range_end) AS last FROM timescaledb_information.chunks WHERE hypertable_name='timeseries';
\echo === chunk interval
SELECT d.column_name, d.time_interval FROM timescaledb_information.dimensions d WHERE hypertable_name='timeseries';
\echo === compression settings and policy
SELECT * FROM timescaledb_information.compression_settings WHERE hypertable_name='timeseries';
SELECT job_id, proc_name, schedule_interval, config, next_start FROM timescaledb_information.jobs WHERE hypertable_name='timeseries';
\echo === per-chunk heap vs index (top 8)
SELECT chunk_name, pg_size_pretty(table_bytes) AS heap, pg_size_pretty(index_bytes) AS idx, pg_size_pretty(total_bytes) AS total FROM chunks_detailed_size('timeseries') ORDER BY total_bytes DESC LIMIT 8;
\echo === column layout and avg row width
SELECT column_name, data_type FROM information_schema.columns WHERE table_name='timeseries' ORDER BY ordinal_position;
SELECT sum(c.reltuples)::bigint AS est_rows, sum(pg_relation_size(c.oid)) AS heap_bytes, round(sum(pg_relation_size(c.oid))/nullif(sum(c.reltuples),0),1) AS bytes_per_row FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='_timescaledb_internal' AND c.relname LIKE '_hyper_%_chunk' AND c.relkind='r';
\echo === dead tuples / bloat signal
SELECT sum(n_live_tup) AS live, sum(n_dead_tup) AS dead, sum(n_tup_upd) AS updates, sum(n_tup_hot_upd) AS hot_updates FROM pg_stat_user_tables WHERE schemaname='_timescaledb_internal' AND relname LIKE '_hyper_%_chunk';
\echo === value kinds (text vs numeric rows)
SELECT count(*) FILTER (WHERE numeric_value IS NOT NULL) AS numeric_rows, count(*) FILTER (WHERE text_value IS NOT NULL) AS text_rows, count(*) FILTER (WHERE numeric_value IS NULL AND text_value IS NULL) AS null_rows, count(*) FILTER (WHERE deleted) AS deleted_rows, count(DISTINCT last_revision) AS distinct_revisions FROM timeseries;
\echo === other tables by size
SELECT relname, pg_size_pretty(pg_total_relation_size(c.oid)) AS total FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND relkind='r' ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 8;
