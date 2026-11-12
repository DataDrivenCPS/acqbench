#!/usr/bin/env bash
P="docker exec acq_bench_tsdb psql -U acquirium -d acquirium -tA -c"
echo "START $(date -u +%FT%TZ) $(date +%s)"
echo "db_before $($P "SELECT pg_database_size(current_database())")"
for ch in $($P "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks WHERE hypertable_name='timeseries' AND NOT is_compressed ORDER BY range_start"); do
  name=${ch#*.}
  before=$($P "SELECT total_bytes FROM chunks_detailed_size('timeseries') WHERE chunk_name='$name'")
  t0=$(date +%s.%N)
  $P "SELECT compress_chunk('$ch')" >/dev/null
  t1=$(date +%s.%N)
  after=$($P "SELECT after_compression_total_bytes FROM chunk_compression_stats('timeseries') WHERE chunk_name='$name'")
  printf "%s before=%s after=%s secs=%.1f\n" "$name" "$before" "$after" "$(echo "$t1 - $t0" | bc)"
done
echo "db_after $($P "SELECT pg_database_size(current_database())")"
echo "END $(date -u +%FT%TZ) $(date +%s)"
