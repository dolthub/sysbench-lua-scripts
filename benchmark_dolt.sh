#!/bin/bash
set -eo pipefail
#set -x

# TODO: in CI this would need to install Dolt or the lua scripts
DOLT_ROOT=/Users/max-hoffman/go/src/github.com/dolthub/dolt
SCRIPTS_ROOT=/Users/max-hoffman/go/src/github.com/dolthub/dolt/go/performance/scripts/sysbench-lua-scripts

ROOT_USER=user
ROOT_PASS=pass
ROOT_TABLE_SIZE=10000
ROOT_TIMEOUT=30
ROOT_EVENTS=200
ROOT_PORT=3309

reads='["oltp_read_only" "oltp_point_select" "select_random_points" "select_random_ranges" "covering_index_scan" "index_scan" "table_scan" "groupby_scan" "index_join_scan"]'
writes='["oltp_read_write" "oltp_update_index" "oltp_update_non_index" "oltp_insert" "bulk_insert" "oltp_write_only" "oltp_delete_insert"]'

DEFAULT_SCRIPTS='["oltp_read_only", "oltp_point_select"]'
DEFAULT_VERSIONS='["main"]'

SCRIPTS="${INPUT_SCRIPTS:-$DEFAULT_SCRIPTS}"
VERSIONS="${INPUT_VERSIONS:-$DEFAULT_VERSIONS}"

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

    dolt_bin="$server_log_dir/dolt_$doltv"
    install_dolt_version $doltv $dolt_bin

    # inline start_server until we can get pid from server lock
    server_log="$server_log_dir/$doltv.log"
    cd $wd
    dolt init >> "$server_log"
    dolt sql -q "create database sbtest" >>"$server_log"
    $dolt_bin sql-server -l trace --user=$ROOT_USER --password=$ROOT_PASS --port "$port" &>"$server_log" &
    sleep 1
    pid="$!"

    echo "$SCRIPTS" | jq -c ".[${i}]" | while read s; do
        #script_name="${s}"
        #script_name=${scripts[$i]}
        script_name=$(echo $s | sed -e 's/^"//' -e 's/"$//' )
        script_log="$script_log_dir/$script_name.log"
        echo "script: $script_name"
        run_script $script_name $script_log $port
    done
    kill_server $pid
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
    RET_SERVER_PID="$!"
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
    sysbench $default_opts "$script_name" prepare >> "$script_log"
    sysbench $default_opts "$script_name" run >> "$script_log"
    sysbench $default_opts "$script_name" cleanup >> "$script_log"
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

list_running_servers() {
    running=$(ps aux | grep "$ROOT_PORT.*dolt" )
    echo "running dolt servers:"
    echo "$running"
}

format_markdown() {
    if [ "$#" -ne 2 ]; then
        echo "format_markdown expects 2 arguments, found ${$#}"
        echo "usage: format_markdown [csv file] [markdown file]"
        exit 1
    fi
    csv=$1
    md=$2

    cnt=$(head -1 $csv | tr -cd , | wc -c)
    header2='|'
    for i in $(seq 0 $cnt); do
      header2="$header2 --- |"
    done

    head -1 $csv \
      | sed 's/^/|\ /g' \
      | sed 's/,/\ |\ /g' \
      | sed 's/$/\ |/g' >> $md
    echo $header2 >> $md
    tail +2 $csv \
      | sed 's/^/|\ /g' \
      | sed 's/,/\ |\ /g' \
      | sed 's/$/\ |/g' >> $md
}

run () {
    tmp_dir=`mktemp -d`
    echo "output dir: $tmp_dir"

    summary_dir="$tmp_dir/summary"
    mkdir -p $summary_dir

    trap list_running_servers EXIT

    echo "$VERSIONS" | jq -c ".[${i}]" | while read v; do
        dolt_version=$(echo $v | sed -e 's/^"//' -e 's/"$//' )
        wd="$tmp_dir/$dolt_version"
        mkdir -p $wd
        cp $SCRIPTS_ROOT/*.lua "$wd/"

        server_logs="$wd/server_logs/"
        script_logs="$wd/script_logs/"
        mkdir -p $server_logs $script_logs

        port=$ROOT_PORT
        benchmark_dolt "$dolt_version" "$server_logs" "$script_logs" "$port"

        hist_logs="$wd/hist_logs"
        mkdir -p $hist_logs
        format_hist $script_logs $hist_logs

        version_summary="$summary_dir/$dolt_version.csv"
        collect_summary $hist_logs $version_summary

        version_md="$summary_dir/$dolt_version.md"
        format_markdown $version_summary $version_md

        echo "summary for $dolt_version at $version_summary"
        cat $version_md
    done
}

run
