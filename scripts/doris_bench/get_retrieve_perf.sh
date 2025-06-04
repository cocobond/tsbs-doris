#!/bin/bash
set -eo pipefail

# 打印使用帮助
show_help() {
    echo "用法: $0 -scale <scale> [-dbtype <dbtype>] [-host <host>]"
    echo ""
    echo "必填参数:"
    echo "  -scale <scale>: 数据规模 (可选值: 100, 4000)"
    echo ""
    echo "可选参数:"
    echo "  -dbtype <dbtype>: 要运行的数据库类型，默认 all"
    echo "      可选值: all, clickhouse, doris, timescaledb, vm, influx"
    echo "  -host <host>: 数据库主机地址，默认 127.0.0.1"
    echo ""
    echo "示例:"
    echo "  $0 -scale 100"
    echo "  $0 -scale 4000 -dbtype influx -host 192.168.1.1"
    exit 1
}

# 默认参数
SCALE=""
DBTYPE="all"
HOST="127.0.0.1"

# 解析命令行参数
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -scale) SCALE="$2"; shift ;;
        -dbtype) DBTYPE="$2"; shift ;;
        -host) HOST="$2"; shift ;;
        *) show_help ;;
    esac
    shift
done

# 参数验证
if [ -z "$SCALE" ]; then
    echo "错误：必须指定 scale 参数！"
    show_help
elif [[ ! "$SCALE" =~ ^(100|4000)$ ]]; then
    echo "错误：scale 必须是 100 或 4000"
    show_help
fi

# 根据设备数量设置时间范围
if [ "$SCALE" -eq 100 ]; then
    TIMESTAMP_START="2024-01-01T00:00:00Z"
    TIMESTAMP_END="2024-02-01T00:00:00Z"
elif [ "$SCALE" -eq 4000 ]; then
    TIMESTAMP_START="2024-01-01T00:00:00Z"
    TIMESTAMP_END="2024-01-05T00:00:00Z"
fi

# 输出主机地址
echo "使用主机地址: $HOST"
VM_URL="http://$HOST:8428"
INFLUX_URL="http://$HOST:8086"

# VictoriaMetrics 跳过的查询类型
VM_SKIP_TYPES=("high-cpu-all" "groupby-orderby-limit" "lastpoint" "high-cpu-1")

# 查询类型列表
QUERY_TYPES=(
    "single-groupby-1-1-1"
    "single-groupby-1-1-12"
    "single-groupby-1-8-1"
    "single-groupby-5-1-1"
    "single-groupby-5-1-12"
    "single-groupby-5-8-1"
    "cpu-max-all-1"
    "cpu-max-all-8"
    "double-groupby-1"
    "double-groupby-5"
    "double-groupby-all"
    "high-cpu-1"
    "high-cpu-all"
    "groupby-orderby-limit"
    "lastpoint"
)

# 创建查询目录
mkdir -p /tsbs/{clickhouse_cpu_query,doris_cpu_query,timescaledb_cpu_query,vm_cpu_query,influx_cpu_query}

echo "生成所有查询文件（Scale: $SCALE）"
echo "时间范围: $TIMESTAMP_START 到 $TIMESTAMP_END"

# 生成所有数据库的查询文件
for query_type in "${QUERY_TYPES[@]}"; do
    # ClickHouse
    ck_file="/tsbs/clickhouse_cpu_query/clickhouse_${query_type}_scale${SCALE}.sql"
    ./tsbs_generate_queries --scale=$SCALE --use-case=cpu-only --format=clickhouse --query-type=$query_type \
        --timestamp-start="$TIMESTAMP_START" --timestamp-end="$TIMESTAMP_END" --file="$ck_file" > /dev/null 2>&1

    # Doris
    doris_file="/tsbs/doris_cpu_query/doris_${query_type}_scale${SCALE}.sql"
    ./tsbs_generate_queries --scale=$SCALE --use-case=cpu-only --format=doris --query-type=$query_type \
        --timestamp-start="$TIMESTAMP_START" --timestamp-end="$TIMESTAMP_END" --file="$doris_file" > /dev/null 2>&1

    # TimescaleDB
    ts_file="/tsbs/timescaledb_cpu_query/timescaledb_${query_type}_scale${SCALE}.sql"
    ./tsbs_generate_queries --scale=$SCALE --use-case=cpu-only --format=timescaledb --query-type=$query_type \
        --timestamp-start="$TIMESTAMP_START" --timestamp-end="$TIMESTAMP_END" --file="$ts_file" > /dev/null 2>&1

    # InfluxDB
    influx_file="/tsbs/influx_cpu_query/influx_${query_type}_scale${SCALE}.txt"
    ./tsbs_generate_queries --scale=$SCALE --use-case=cpu-only --format=influx --query-type=$query_type \
        --timestamp-start="$TIMESTAMP_START" --timestamp-end="$TIMESTAMP_END" --file="$influx_file" > /dev/null 2>&1

    # VictoriaMetrics（跳过部分）
    if [[ ! " ${VM_SKIP_TYPES[@]} " =~ " ${query_type} " ]]; then
        vm_file="/tsbs/vm_cpu_query/vm_${query_type}_scale${SCALE}.txt"
        ./tsbs_generate_queries --scale=$SCALE --use-case=cpu-only --format=victoriametrics --query-type=$query_type \
            --timestamp-start="$TIMESTAMP_START" --timestamp-end="$TIMESTAMP_END" --file="$vm_file" > /dev/null 2>&1
    fi
done

# 定义统一的查询执行函数
run_queries() {
    local db_name="$1"
    local format="$2"
    local host="$3"
    local query_dir="/tsbs/${db_name}_cpu_query"
    local output_times=()
    local total_mean=0

    echo "✅ 开始执行 ${db_name} 查询..."

    for query_type in "${QUERY_TYPES[@]}"; do
        # VictoriaMetrics 跳过某些类型
        if [[ "$db_name" == "victoriametrics" && " ${VM_SKIP_TYPES[@]} " =~ " ${query_type} " ]]; then
            echo "⚠️ 跳过 VictoriaMetrics 不支持的查询类型: $query_type"
            continue
        fi

        local query_file="${query_dir}/${db_name}_${query_type}_scale${SCALE}.${format}"
        if [ ! -f "$query_file" ]; then
            echo "⚠️ 查询文件不存在: $query_file"
            continue
        fi

        echo "🚀 准备执行 ${db_name} ${query_type} 查询..."
        local TS=$(date +%s)
        local result
        case "$db_name" in
            clickhouse)
                result=$(./tsbs_run_queries_clickhouse --file="$query_file" --hosts="$host" --max-queries=100 2>&1)
                ;;
            doris)
                result=$(./tsbs_run_queries_doris --file="$query_file" --hosts="$host" --max-queries=100 2>&1)
                ;;
            timescaledb)
                result=$(./tsbs_run_queries_timescaledb --file="$query_file" --hosts="$host" --max-queries=10 2>&1)
                ;;
            victoriametrics)
                result=$(./tsbs_run_queries_victoriametrics --file="$query_file" --urls="$VM_URL" --max-queries=100 2>&1)
                ;;
            influx)
                result=$(./tsbs_run_queries_influx --file="$query_file" --urls="$INFLUX_URL" --max-queries=10 2>&1)
                ;;
        esac

        local duration=$(( $(date +%s) - TS ))

        # 提取第一行包含 min/med/mean/max 的那一行
        local stats_line=$(echo "$result" | grep -E 'min:|med:|mean:|max:' | head -n 1)

        local min=$(echo "$stats_line" | grep -oP 'min:\s*\K[0-9.]+')
        local med=$(echo "$stats_line" | grep -oP 'med:\s*\K[0-9.]+')
        local mean=$(echo "$stats_line" | grep -oP 'mean:\s*\K[0-9.]+')
        local max=$(echo "$stats_line" | grep -oP 'max:\s*\K[0-9.]+')

        # 验证 mean 是否有效
        if [[ -z "$mean" || ! "$mean" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
            echo "⚠️ 警告：查询 ${query_type} 的 mean 值无效: '$mean'，跳过累加"
            mean=0
        fi

        output_times+=("$query_type: $duration 秒 (min: $min, mean: $mean, med: $med, max: $max)")
        total_mean=$(echo "$total_mean + $mean" | bc -l)

        sleep 5
    done

    echo "✅ ${db_name} 所有查询完成，详细耗时如下："
    for entry in "${output_times[@]}"; do
        echo "  $entry"
    done

    local formatted_total=$(printf "%.2f" "$total_mean")
    echo "📊 ${db_name} 所有场景 mean 总和: ${formatted_total} ms"
    echo "⚙️--服务器休息3m---------------------"
    sleep 180
}

# 根据 dbtype 参数选择要运行的数据库
case "$DBTYPE" in
    all)
        run_queries "clickhouse" "sql" "$HOST"
        run_queries "doris" "sql" "$HOST"
        run_queries "timescaledb" "sql" "$HOST"
        run_queries "victoriametrics" "txt" "$HOST"
        run_queries "influx" "txt" "$HOST"
        ;;
    clickhouse)
        run_queries "clickhouse" "sql" "$HOST"
        ;;
    doris)
        run_queries "doris" "sql" "$HOST"
        ;;
    timescaledb)
        run_queries "timescaledb" "sql" "$HOST"
        ;;
    vm)
        run_queries "victoriametrics" "txt" "$HOST"
        ;;
    influx)
        run_queries "influx" "txt" "$HOST"
        ;;
    *)
        echo "❌ 错误：不支持的 dbtype 类型: $DBTYPE"
        show_help
        ;;
esac

echo "🎉 所有查询执行完成！scale=${SCALE}"