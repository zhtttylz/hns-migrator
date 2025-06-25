#!/bin/bash

set -euo pipefail

script_dir="$(dirname "$0")"
input_file="$script_dir/hdfs_owners.log"

if [ ! -f "$input_file" ]; then
  echo "Input file $input_file not found" >&2
  exit 1
fi

mapfile -t lines < "$input_file"

# 仅仅处理到表前面路径这一层级
# 如表路径为"hdfs://DClusterUS1/user/growth_data/growth_data/hive/growth_data/dwd_ut_log_resource_zhunxing_req_issue_di"
# 只校验到 “hdfs://DClusterUS2/user/growth_data/growth_data/hive/growth_data”
limit=$((${#lines[@]} - 1))

for ((i=0; i<limit; i++)); do
  line="${lines[$i]}"
  path=$(echo "$line" | awk '{print $1}')
  owner=$(echo "$line" | awk '{print $2}')
  group=$(echo "$line" | awk '{print $3}')
  # 这里固定为美东
  target=$(echo "$path" | sed "s/DClusterUS1/DClusterUS2/g")
  echo "===================================================="
  if ! hadoop fs -ls -d "$target" >/dev/null 2>&1; then
    echo "hadoop fs -mkdir $target"
    echo "hadoop fs -chown $owner:$group $target"
  fi
done

last_line=$(tail -n 1 "$input_file")

#取hdfs_owners.log中的倒数第二行（库这一级别）的owner和group，作为ns2这一层级的owner和group
second_last_line=$(tail -n 2 "$input_file" | head -n 1)


path=$(echo "$last_line" | awk '{print $1}')
owner=$(echo "$last_line" | awk '{print $2}')
group=$(echo "$last_line" | awk '{print $3}')

base_owner=$(echo "$second_last_line" | awk '{print $2}')
base_group=$(echo "$second_last_line" | awk '{print $3}')

target_ns2=$(echo "$path" | sed "s/DClusterUS1/DClusterUS2/g")
base_dir=$(dirname "$target_ns2")
table_name=$(basename "$target_ns2")
ns2_base="$base_dir/ns2"
ns2_path="$ns2_base/$table_name"

echo "===================================================="
if ! hadoop fs -ls -d "$ns2_base" >/dev/null 2>&1; then
  echo "hadoop fs -mkdir $ns2_base"
  echo "hadoop fs -chown $base_owner:$base_group $ns2_base"
fi

echo "===================================================="
if ! hadoop fs -ls -d "$ns2_path" >/dev/null 2>&1; then
  echo "hadoop fs -mkdir $ns2_path"
  echo "hadoop fs -chown $owner:$group $ns2_path"
fi
