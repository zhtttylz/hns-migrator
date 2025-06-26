#!/bin/bash

# 查询所有分区并生成修改 location 的语句
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <database.table>" >&2
  exit 1
fi

table=$1

# 获取表的 location
TABLE_LOC=$(spark-sql --master local -e "DESC FORMATTED $table" 2>/dev/null | grep -i '^Location' | awk '{print $2}')
if [ -z "$TABLE_LOC" ]; then
  echo "Failed to get location for table $table" >&2
  exit 1
fi

BASE_DIR=$(dirname "$TABLE_LOC")
NEW_BASE_DIR=${BASE_DIR/DClusterUS1/DClusterUS2}

# 查询所有分区，将其放入数组，并打印分区数量
# 去除show partition时，打印的”partition“ 字段
mapfile -t parts < <(spark-sql --master local -e "SHOW PARTITIONS $table" 2>/dev/null | grep -v -i -x 'partition' )
echo "总共 ${#parts[@]} 个分区"

for part in "${parts[@]}"; do
  # 将 part 形如 k=v/k2=v2 转为 k=v,k2=v2
  spec=$(echo "$part" | sed 's#/#, #g')
  echo "扫描分区${spec}对应的分区路径"
  loc=$(spark-sql --master local -e "DESC FORMATTED $table PARTITION ($spec)" 2>/dev/null | grep -i '^Location' | awk '{print $2}')
  echo "${spec}的分区路径为${loc}"
  if [[ -z "$loc" ]]; then
    echo "Skip partition $part: cannot get location" >&2
    continue
  fi

  if [[ $loc != hdfs://DClusterUS1* ]]; then
    echo "Skip partition $part: location $loc does not start with hdfs://DClusterUS1" >&2
    continue
  fi

  rest=${loc#"${BASE_DIR}/"}
  if [[ $rest == ns2/* ]]; then
    new_loc="$NEW_BASE_DIR/$rest"
  else
    new_loc="$NEW_BASE_DIR/ns2/$rest"
  fi

  echo "ALTER TABLE $table PARTITION ($spec) SET LOCATION '$new_loc';"
done

