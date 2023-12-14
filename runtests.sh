#!/bin/bash

run_benchmark() {
    benchmark_name=$1
    sysbench_command="sysbench --db-driver=pgsql --pgsql-host=0.0.0.0 --pgsql-password=sbtest "
    echo "Running benchmark: $benchmark_name"
    $sysbench_command $benchmark_name prepare
    $sysbench_command $benchmark_name run
    $sysbench_command $benchmark_name cleanup
}

benchmarks=(
    covering_index_scan_postgres
    groupby_scan_postgres
    index_join_postgres
    index_join_scan_postgres
    index_scan_postgres
    oltp_point_select
    oltp_read_only
    select_random_points
    select_random_ranges
    table_scan_postgres
    types_table_scan_postgres
    oltp_delete_insert
    oltp_insert
    oltp_read_write
    oltp_update_index
    oltp_update_non_index
    oltp_write_only
    types_delete_insert_postgres
)

# Run each benchmark once
for benchmark in "${benchmarks[@]}"; do
    run_benchmark "$benchmark"
done
