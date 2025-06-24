#!/bin/bash

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <new-cluster>" >&2
  exit 1
fi

new_cluster=$1
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
  target=$(echo "$path" | sed "s/DClusterUS1/$new_cluster/g")

  if ! hadoop fs -ls -d "$target" >/dev/null 2>&1; then
    echo "hadoop fs -mkdir $target"
    echo "hadoop fs -chown $owner:$group $target"
  fi

done
