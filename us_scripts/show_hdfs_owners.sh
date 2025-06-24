#!/bin/bash

# 开启"严格模式"
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <hdfs-path>" >&2
  exit 1
fi

full_path=$1

prefix=$(echo "$full_path" | grep -o '^hdfs://[^/]*' || true)
path_without_prefix=$(echo "$full_path" | sed -e 's#^hdfs://[^/]\+##')

current=""
IFS='/' read -ra parts <<< "$path_without_prefix"

output_file="$(dirname "$0")/hdfs_owners.log"
: > "$output_file"

for part in "${parts[@]}"; do
  current="$current/$part"
  target="${prefix}${current}"
  info=$(hadoop fs -ls -d "$target" 2>/dev/null)
  owner=$(echo "$info" | awk '{print $3}')
  group=$(echo "$info" | awk '{print $4}')
  echo "$target $owner $group" | tee -a "$output_file"
done
