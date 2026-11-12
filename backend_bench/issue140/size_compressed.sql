\echo === database
SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size, pg_database_size(current_database()) AS db_bytes;
\echo === hypertable detailed size (compressed)
SELECT table_bytes, index_bytes, toast_bytes, total_bytes FROM hypertable_detailed_size('timeseries');
\echo === compression stats
SELECT total_chunks, number_compressed_chunks, before_compression_table_bytes, before_compression_index_bytes, before_compression_toast_bytes, before_compression_total_bytes, after_compression_table_bytes, after_compression_index_bytes, after_compression_toast_bytes, after_compression_total_bytes FROM hypertable_compression_stats('timeseries');
\echo === per-index size after compression
SELECT indexname, hypertable_index_size(indexname::regclass) AS bytes FROM pg_indexes WHERE tablename='timeseries';
\echo === compressed internal chunks: heap / index / toast
SELECT sum(pg_relation_size(c.oid)) AS compressed_heap_bytes, sum(pg_indexes_size(c.oid)) AS compressed_index_bytes, sum(coalesce(pg_total_relation_size(c.reltoastrelid),0)) AS compressed_toast_bytes, sum(pg_total_relation_size(c.oid)) AS compressed_total_bytes, count(*) AS n FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='_timescaledb_internal' AND c.relname LIKE 'compress_hyper_%_chunk' AND c.relkind='r';
\echo === per-chunk compression (top 8 by before)
SELECT chunk_name, pg_size_pretty(before_compression_total_bytes) AS before, pg_size_pretty(after_compression_total_bytes) AS after, round(before_compression_total_bytes::numeric/after_compression_total_bytes,1) AS ratio FROM chunk_compression_stats('timeseries') ORDER BY before_compression_total_bytes DESC LIMIT 8;
\echo === rows still readable
SELECT count(*) FROM timeseries;
\echo === wal
SELECT pg_size_pretty(sum(size)) AS wal_size FROM pg_ls_waldir();
\echo === db size by schema (where the bytes live now)
SELECT n.nspname, pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS total, sum(pg_total_relation_size(c.oid)) AS bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','i','t','m') GROUP BY 1 ORDER BY 3 DESC LIMIT 6;
