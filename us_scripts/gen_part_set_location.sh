#!/bin/bash

# 查询所有分区并生成修改 location 的语句
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 <database.table> [concurrency]" >&2
  exit 1
fi

table=$1
concurrency=${2:-4}
table_name=${table##*.}

if ! [[ $concurrency =~ ^[0-9]+$ ]] || [ "$concurrency" -le 0 ]; then
  echo "Invalid concurrency: $concurrency" >&2
  exit 1
fi

# 获取表的 location
TABLE_LOC=$(spark-sql --master local -e "DESC FORMATTED $table" 2>/dev/null | grep -i '^Location' | awk '{print $2}')
if [ -z "$TABLE_LOC" ]; then
  echo "Failed to get location for table $table" >&2
  exit 1
fi


# 查询所有分区，将其放入数组，并打印分区数量
# 去除show partition时，打印的”partition“ 字段
mapfile -t parts < <(spark-sql --master local -e "SHOW PARTITIONS $table" 2>/dev/null | grep -v -i -x 'partition' )
echo "总共 ${#parts[@]} 个分区"

process_part() {
  local part="$1"
  local spec loc new_loc

  # 将 part 形如 k=v/k2=v2 转为 k='v', k2='v2'
  spec=$(echo "$part" | sed 's#/#, #g' | sed "s/=\([^,]*\)/='\1'/g")
  echo "扫描分区${spec}对应的分区路径"
  loc=$(spark-sql --master local -e "DESC FORMATTED $table PARTITION ($spec)" 2>/dev/null | grep -i '^Location' | awk '{print $2}' | grep -v -i 'DClusterUS2')
  echo "${spec}的分区路径为${loc}"

  if [[ -z "$loc" ]]; then
    echo "Skip partition $part: cannot get location" >&2
    return
  fi

  if [[ $loc != hdfs://DClusterUS1* ]]; then
    echo "Skip partition $part: location $loc does not start with hdfs://DClusterUS1" >&2
    return
  fi

  new_loc=${loc/DClusterUS1/DClusterUS2}
  if [[ $new_loc != */ns2/${table_name}/* ]]; then
    new_loc=${new_loc/"/${table_name}/"/"/ns2/${table_name}/"}
  fi

  echo "ALTER TABLE $table PARTITION ($spec) SET LOCATION '$new_loc';"
}

for part in "${parts[@]}"; do
  while [ "$(jobs -rp | wc -l)" -ge "$concurrency" ]; do
    sleep 0.2
  done
  process_part "$part" &
done

wait

