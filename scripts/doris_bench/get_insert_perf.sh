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
TSDB_HOST="$HOST"
TSDB_PORT="5432"
TSDB_USER="postgres"
TSDB_PASS=""
VM_URL="http://$HOST:8428/write"
INFLUX_URL="http://$HOST:8086"
DORIS_URL="http://$HOST:8030"
CLICKHOUSE_HOST="$HOST"

# 根据设备数量设置配置
case $SCALE in
    100)
        # 100 devices
        end_time="2024-02-01T00:00:00Z"
        # TimescaleDB 参数
        chunk_time="62h"       # 2.58天 ≈ 62小时
        expected_rows=26784000 # 100设备的预期行数
        ;;
    4000)
        # 4000 devices
        end_time="2024-01-05T00:00:00Z"
        # TimescaleDB 参数
        chunk_time="8h"        # 精确匹配场景描述
        expected_rows=138240000 # 4000设备的预期行数
        ;;
esac

# 定义输出文件名前缀
prefix="scale_${SCALE}"
ck_output_file="${prefix}_ck_data.csv"
vm_output_file="${prefix}_vm_data.bulk"
doris_prefix="doris_${SCALE}"

# 根据数据库类型设置执行标志
run_clickhouse=false
run_doris=false
run_timescaledb=false
run_vm=false
run_influx=false

case $DBTYPE in
    all)
        run_clickhouse=true
        run_doris=true
        run_timescaledb=true
        run_vm=true
        run_influx=true
        ;;
    clickhouse) run_clickhouse=true ;;
    doris) run_doris=true ;;
    timescaledb) run_timescaledb=true ;;
    vm) run_vm=true ;;
    influx) run_influx=true ;;
    *)
        echo "错误：无效的数据库类型: $DBTYPE"
        show_help
        ;;
esac

echo "=============================================="
echo "开始执行性能测试"
echo "设备数量: $SCALE"
echo "数据库类型: $DBTYPE"
echo "时间范围: 2024-01-01T00:00:00Z 到 $end_time"
echo "=============================================="

# 生成数据处理脚本
generate_process_script() {
    local output_file=$1
    local doris_prefix=$2
    local doris_tags_file="${doris_prefix}_tags.csv"
    local doris_cpu_file="${doris_prefix}_cpu.csv"

    cat > process.py <<EOF
import time
from datetime import datetime, timezone

start_time = time.time()

input_filename = '${output_file}'
tags_dict = {}  # 存储已出现的tag内容和对应ID
current_tag_id = None

# 定义输出文件名
tags_filename = '${doris_tags_file}'
cpu_filename = '${doris_cpu_file}'

with open(input_filename, 'r', encoding='utf-8') as infile, \
        open(tags_filename, 'w', encoding='utf-8') as tags_file, \
        open(cpu_filename, 'w', encoding='utf-8') as cpu_file:

    # 跳过前两行头信息
    _ = infile.readline()  # 第一行
    _ = infile.readline()  # 第二行

    for line in infile:
        line = line.strip()
        if not line:
            continue

        if line.startswith('tags'):
            tag_content = line[5:]

            if tag_content in tags_dict:
                current_tag_id = tags_dict[tag_content]
            else:
                current_tag_id = len(tags_dict) + 1  # ID从1开始
                tags_dict[tag_content] = current_tag_id

                # 解析字段
                field_mapping = {key: '' for key in [
                    'hostname', 'region', 'datacenter', 'rack',
                    'os', 'arch', 'team', 'service',
                    'service_version', 'service_environment'
                ]}

                for field in tag_content.split(','):
                    if '=' in field:
                        key, value = field.split('=', 1)
                        if key in field_mapping:
                            # 去除可能存在的引号
                            field_mapping[key] = value.replace('"', '').replace("'", "")

                # 直接写入数据行（无表头）
                tags_file.write(','.join([
                    str(current_tag_id),
                    field_mapping['hostname'],
                    field_mapping['region'],
                    field_mapping['datacenter'],
                    field_mapping['rack'],
                    field_mapping['os'],
                    field_mapping['arch'],
                    field_mapping['team'],
                    field_mapping['service'],
                    field_mapping['service_version'],
                    field_mapping['service_environment']
                ]) + '\n')

        elif line.startswith('cpu'):
            if current_tag_id is None:
                continue

            parts = line.split(',')
            if len(parts) < 12:
                print(f"无效的CPU数据行：{line}")
                continue

            # 处理时间字段（第二列）
            original_time_str = parts[1].strip()
            try:
                # 将纳秒时间戳转换为整数
                timestamp_ns = int(original_time_str)

                # 转换为秒（浮点数）
                timestamp_seconds = timestamp_ns / 1_000_000_000

                # 创建UTC时间对象
                dt_utc = datetime.utcfromtimestamp(timestamp_seconds)

                # 格式化为Doris所需的日期时间格式
                created_at = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
                created_date = dt_utc.strftime("%Y-%m-%d")
            except Exception as e:
                print(f"时间解析失败：{original_time_str}，错误：{e}")
                continue

            # 提取指标值（第3到第12列）
            metrics = parts[2:12]

            # 确保有10个指标值（不足则填充空值）
            if len(metrics) < 10:
                metrics.extend([''] * (10 - len(metrics)))

            # 组装新的CPU数据行（按表结构顺序）
            new_cpu_line = [
                str(current_tag_id),      # tags_id (bigint)
                created_at,               # created_at (datetime)
                created_date,             # created_date (date)
                original_time_str,        # time (bigint, 原始纳秒时间戳)
                *metrics,                 # 10个usage指标
            ]

            # 写入CPU数据行
            cpu_file.write(','.join(new_cpu_line) + '\n')

end_time = time.time()
print(f"脚本执行时间：{end_time - start_time:.2f} 秒")
print(f"生成唯一tag数量：{len(tags_dict)}")
print(f"生成的tags文件: {tags_filename}")
print(f"生成的cpu文件: {cpu_filename}")
EOF
}

# Doris 加载函数
load_doris_data() {
    local doris_prefix=$1
    local expected_rows=$2

    local doris_tags_file="${doris_prefix}_tags.csv"
    local doris_cpu_file="${doris_cpu_file}"

    echo "开始加载到Doris..."
    echo "doris_tags_loading..."
    response=$(curl --silent --location-trusted -u root:"" \
        -H "Expect: 100-continue" \
        -H "column_separator: ," \
        -H "columns: tags_id,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment" \
        -T "$doris_tags_file" \
        ${DORIS_URL}/api/benchmark/tags/_stream_load)
    echo "Tags 加载结果: $response"

    # 计算CPU文件行数
    if [ "$expected_rows" -eq 0 ]; then
        expected_rows=$(wc -l < "$doris_cpu_file")
        echo "计算得到的CPU文件行数: $expected_rows"
    fi

    # 创建分割文件目录
    split_dir="split_cpu_files"
    mkdir -p "$split_dir"

    # 计算每个分片应该包含的行数
    lines_per_file=$(( (expected_rows + 31) / 32 ))  # 向上取整
    echo "将CPU文件分割为32个分片，每个分片约 $lines_per_file 行"

    # 分割CPU文件
    split -l $lines_per_file -d -a 3 "$doris_cpu_file" "${split_dir}/part_"

    # 获取分割后的文件列表
    split_files=("${split_dir}/part_"*)
    file_count=${#split_files[@]}
    echo "实际分割文件数量: $file_count"

    # 创建用于存储PID和结果的临时文件
    pids=()
    result_file="doris_import_results.txt"
    > "$result_file"  # 清空结果文件

    # 记录开始时间
    start_time_global=$(date +%s.%N)

    # 并发导入分割文件
    for file in "${split_files[@]}"; do
        (
            # 记录单个文件的开始时间
            start_time=$(date +%s.%N)

            # 执行导入
            response=$(curl --silent --location-trusted -u root:"" \
                -H "Expect: 100-continue" \
                -H "column_separator: ," \
                -H "columns:tags_id,created_at,created_date,time,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice" \
                -T "$file" \
                ${DORIS_URL}/api/benchmark/cpu/_stream_load)

            # 记录结束时间
            end_time=$(date +%s.%N)
            duration=$(awk "BEGIN {printf \"%.2f\", $end_time - $start_time}")

            # 解析响应中的实际加载行数
            rows_loaded=$(echo "$response" | jq -r '.NumberLoadedRows')
            if [ "$rows_loaded" == "null" ]; then
                rows_loaded=0
            fi

            # 写入结果
            echo "$file,$rows_loaded,$duration,$response" >> "$result_file"
        ) &
        pids+=($!)
    done

    # 等待所有后台任务完成
    wait "${pids[@]}"

    # 记录结束时间
    end_time_global=$(date +%s.%N)

    # 计算总耗时
    total_duration=$(awk "BEGIN {printf \"%.2f\", $end_time_global - $start_time_global}")

    # 汇总结果
    total_rows_loaded=0
    total_files=0
    success_files=0

    while IFS=, read -r file rows duration response; do
        total_rows_loaded=$((total_rows_loaded + rows))
        total_files=$((total_files + 1))

        # 检查是否成功
        if echo "$response" | grep -q '"Status":"Success"'; then
            success_files=$((success_files + 1))
        else
            echo "文件导入失败: $file, 响应: $response"
        fi
    done < "$result_file"

    # 计算导入速率
    if [ "$total_duration" != "0" ]; then
        rows_per_sec=$(awk "BEGIN {printf \"%.2f\", $total_rows_loaded / $total_duration}")
    else
        rows_per_sec="N/A"
    fi

    # 打印汇总信息
    echo "========================================"
    echo "Doris CPU数据并发导入汇总"
    echo "总文件数: $total_files"
    echo "成功文件数: $success_files"
    echo "总加载行数: $total_rows_loaded"
    echo "总耗时: ${total_duration} 秒"
    echo "平均速率: ${rows_per_sec} 行/秒"
    echo "========================================"

    # 清理临时文件
    rm -rf "$split_dir"
    rm "$result_file"
}

# 主执行流程
main() {
    # 生成ClickHouse格式数据（用于ClickHouse, Doris, TimescaleDB）
    if $run_clickhouse || $run_doris || $run_timescaledb; then
        echo "生成ClickHouse格式测试数据（设备数: $SCALE）..."
        ./tsbs_generate_data --use-case="cpu-only" \
          --format="clickhouse" \
          --log-interval="10s" \
          --scale=$SCALE \
          --timestamp-start="2024-01-01T00:00:00Z" \
          --timestamp-end="$end_time" \
          --file="$ck_output_file" \
          --seed=123

        # 生成Doris数据处理脚本
        generate_process_script "$ck_output_file" "$doris_prefix"
    fi

    # 生成VictoriaMetrics格式数据（用于VM, InfluxDB）
    if $run_vm || $run_influx; then
        echo "生成VictoriaMetrics格式测试数据（设备数: $SCALE）..."
        ./tsbs_generate_data --use-case="cpu-only" \
          --format="victoriametrics" \
          --log-interval="10s" \
          --scale=$SCALE \
          --timestamp-start="2024-01-01T00:00:00Z" \
          --timestamp-end="$end_time" \
          --file="$vm_output_file" \
          --seed=123
    fi

    # ClickHouse 加载
    if $run_clickhouse; then
        echo "开始加载到ClickHouse..."
        echo "ClickHouse 加载 (workers=32):"
        TS_START=$(date +%s)
        ./tsbs_load_clickhouse --file="$ck_output_file" --host="$CLICKHOUSE_HOST" --workers=32
        DURATION=$(( $(date +%s) - TS_START ))
        echo "ClickHouse 加载完成，耗时: ${DURATION}秒"
        echo "----------------------------------------------"
        sleep 10
    fi

    # Doris 加载
    if $run_doris; then
        echo "Doris: 预处理伪csv-->csv中..."
        python3 process.py

        # 定义Doris输出文件名
        doris_tags_file="${doris_prefix}_tags.csv"
        doris_cpu_file="${doris_prefix}_cpu.csv"

        # 执行Doris加载
        load_doris_data "$doris_prefix" "$expected_rows"
        echo "----------------------------------------------"
        sleep 10
    fi

    # TimescaleDB 加载
    if $run_timescaledb; then
        echo "开始加载到TimescaleDB..."
        echo "TimescaleDB 加载 (workers=32):"
        TS_START=$(date +%s)
        ./tsbs_load_timescaledb \
          --host="$TSDB_HOST" \
          --port="$TSDB_PORT" \
          --user="$TSDB_USER" \
          --pass="$TSDB_PASS" \
          --file="$ck_output_file" \
          --chunk-time="$chunk_time" \
          --batch-size=10000 \
          --time-index \
          --partition-index \
          --workers=32
        DURATION=$(( $(date +%s) - TS_START ))
        echo "TimescaleDB 加载完成，耗时: ${DURATION}秒"
        echo "----------------------------------------------"
        sleep 10
    fi

    # VictoriaMetrics 加载
    if $run_vm; then
        echo "开始加载到VictoriaMetrics..."
        echo "VictoriaMetrics 加载 (workers=32):"
        TS_START=$(date +%s)
        ./tsbs_load_victoriametrics \
          --file="$vm_output_file" \
          --urls="$VM_URL" \
          --workers=32
        DURATION=$(( $(date +%s) - TS_START ))
        echo "VictoriaMetrics 加载完成，耗时: ${DURATION}秒"
        echo "----------------------------------------------"
        sleep 10
    fi

    # InfluxDB 加载
    if $run_influx; then
        echo "开始加载到InfluxDB..."
        echo "InfluxDB 加载 (workers=32):"
        TS_START=$(date +%s)
        ./tsbs_load_influx \
          --file="$vm_output_file" \
          --urls="$INFLUX_URL" \
          --workers=32
        DURATION=$(( $(date +%s) - TS_START ))
        echo "InfluxDB 加载完成，耗时: ${DURATION}秒"
        echo "----------------------------------------------"
        sleep 10
    fi

    echo "=============================================="
    echo "所有选定的数据库加载测试完成！"
    echo "设备数量: $SCALE"
    echo "数据库类型: $DBTYPE"
    echo "=============================================="
}

# 执行主函数
main