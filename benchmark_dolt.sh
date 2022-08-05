#!/bin/bash
set -eo pipefail

DOLT_ROOT=/Users/max-hoffman/go/src/github.com/dolthub/dolt
SCRIPTS_ROOT=/Users/max-hoffman/go/src/github.com/dolthub/dolt/go/performance/scripts/sysbench-lua-scripts
ROOT_USER=user
ROOT_PASS=pass
ROOT_TABLE_SIZE=10000
ROOT_TIMEOUT=30
ROOT_EVENTS=200

benchmark_dolt() {
    # version, wd, port
    if [ "$#" -ne 4 ]; then
        echo "benchmark_dolt expects 5 arguments, found ${$#}"
        echo "usage: benchmark_dolt [committish] [script logs dir] [server logs dir] [port]"
        exit 1
    fi
    doltv=$1
    server_log_dir=$2
    script_log_dir=$3
    port=$4
    #scripts=('oltp_read_only' 'oltp_point_select')
    #scripts=('oltp_delete_insert' 'groupby_scan' 'covering_index_scan' 'index_join_scan' 'index_scan' 'table_scan' 'types_delete_insert' 'types_table_scan')
    scripts=('oltp_read_only' 'oltp_point_select' 'select_random_points' 'select_random_ranges' 'covering_index_scan' 'index_scan' 'table_scan' 'groupby_scan' 'index_join_scan')
    #writeTests=('oltp_read_write' 'oltp_update_index' 'oltp_update_non_index' 'oltp_insert' 'bulk_insert' 'oltp_write_only' 'oltp_delete_insert')

    dolt_bin="$server_log_dir/dolt_$doltv"
    install_dolt_version $doltv $dolt_bin

    server_log="$server_log_dir/$doltv.log"
    cd $wd
    dolt init
    dolt sql -q "create database sbtest"
    start_server $dolt_bin $wd $port $server_log

    #SERVER_PID="$!"

    END=${#scripts[@]}
    for ((i=0;i<=END-1;i++)); do
        script_name=${scripts[$i]}
        script_log="$script_log_dir/$script_name.log"
        echo "script: $script_name"
        run_script $script_name $script_log $port
    done

    #kill_server $SERVER_PID
}

install_dolt_version() {
    if [ "$#" -ne 2 ]; then
        echo "install_dolt_version expects 2 arguments, found ${$#}"
        echo "usage: install_dolt_version [committish] [dolt_bin]"
        exit 1
    fi
    doltv=$1
    dolt_bin=$2
    cd $DOLT_ROOT/go
    git checkout $doltv
    go build -o $dolt_bin $DOLT_ROOT/go/cmd/dolt
}

start_server() {
    if [ "$#" -ne 4 ]; then
        echo "start_server expects 3 arguments, found $#"
        echo "usage: start_server [dolt_bin] [data dir] [port] [server log]"
        exit 1
    fi
    dolt_bin=$1
    wd=$2
    port=$3
    server_log=$4

    $dolt_bin sql-server -l trace --user=$ROOT_USER --password=$ROOT_PASS --port "$port" &>"$server_log" &
    sleep 1
}

kill_server() {
    pid=$1
    kill -15 $pid
}

run_script() {
    if [ "$#" -ne 3 ]; then
        echo "run_script expects 3 arguments, found ${$#}"
        echo "usage: run_script [name] [log file] [port]"
        exit 1
    fi
    script_name=$1
    script_log=$2
    port=$3

    default_opts="
      --db-driver="mysql" \
      --mysql-host="0.0.0.0" \
      --mysql-port="$port" \
      --mysql-user="$ROOT_USER" \
      --mysql-password="$ROOT_PASS" \
      --mysql-port="$port" \
      --rand-seed=1 \
      --table-size=$ROOT_TABLE_SIZE \
      --rand-type=uniform \
      --events=$ROOT_EVENTS \
      --time=$ROOT_TIMEOUT \
      --histogram=on
    "
    pwd
    sysbench $default_opts "$script_name" prepare
    sysbench $default_opts "$script_name" run > "$script_log"
    sysbench $default_opts "$script_name" cleanup
}

format_hist() {
    if [ "$#" -ne 2 ]; then
        echo "format_hist expects 2 arguments, found ${$#}"
        echo "usage: format_hist [log directory] [output directory] [port]"
        exit 1
    fi
    log_dir=$1
    hist_dir=$2

    for f in $log_dir/*.log; do
        new_file="$hist_dir/$(basename -s .log $f).csv"
        # filter for histogram | convert histogram into csv
        cat $f | sed -n '/Latency histogram/,/SQL statistics/p' | tail -n +2 | tail -r | tail -n +3 | tail -r | awk '{ printf "%s,%s\n", $1, $3 }' > $new_file
    done
}

print_stats="
import os, csv, sys, statistics
file = sys.argv[1]
name = os.path.basename(file)[:-4] # remove .csv postfix
values = []
for i, row in enumerate(csv.reader(open(file), delimiter=',')):
    if i == 0: continue
    for _ in range(int(row[1])):
        values.append(float(row[0]))
stdev = statistics.stdev(values)
mean = statistics.mean(values)
print(f'{name},{mean:.2f},{stdev:.2f}')
"

collect_summary() {
    if [ "$#" -ne 2 ]; then
        echo "collect_summary expects 2 arguments, found ${$#}"
        echo "usage: collect_summary [log directory] [output directory] [port]"
        exit 1
    fi
    hist_logs=$1
    summary_file=$2

    echo "script,mean,var" > $summary_file
    for f in $hist_logs/*.csv; do
        python3 -c "$print_stats" $f >> $summary_file
    done
}

run () {
    #TODO  get versions from ENV variables
    #TODO get scripts from env variables
    versions=('v0.40.21' 'v0.40.20')

    tmp_dir=`mktemp -d`
    echo "output dir: $tmp_dir"

    summary_dir="$tmp_dir/summary"
    mkdir -p $summary_dir

    VERLEN=${#versions[@]}
    for ((j=0;j<=VERLEN-1;j++)); do
        dolt_version=${versions[$j]}
        wd="$tmp_dir/$dolt_version"
        mkdir -p $wd
        cp $SCRIPTS_ROOT/*.lua "$wd/"
        server_logs="$wd/server_logs/"
        script_logs="$wd/script_logs/"
        mkdir -p $server_logs $script_logs
        port=3309

        benchmark_dolt "$dolt_version" "$server_logs" "$script_logs" "$port"

        hist_logs="$wd/hist_logs"
        mkdir -p $hist_logs
        format_hist $script_logs $hist_logs

        version_summary="$summary_dir/$dolt_version.csv"
        collect_summary $hist_logs $version_summary
        echo "summary for $dolt_version at $version_summary"
        cat $version_summary
    done
}

run
