#!/bin/bash

# 查询所有分区并生成修改 location 的语句
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <database.table>" >&2
  exit 1
fi

table=$1

db_name=${table%%.*}
table_name=${table##*.}

# 获取库的location
DB_LOC=$(spark-sql --master local -e "DESC DATABASE EXTENDED $db_name" 2>/dev/null | grep -i '^Location:' | awk '{print $2}')
if [ -z "$DB_LOC" ]; then
  echo "Failed to get location for database $db_name" >&2
  exit 1
fi

# 目标库路径，替换集群名称
NEW_DB_LOC=${DB_LOC/DClusterUS1/DClusterUS2}

# 查询所有分区
mapfile -t parts < <(spark-sql --master local -e "SHOW PARTITIONS $table" 2>/dev/null)

for part in "${parts[@]}"; do
  # 将 part 形如 k=v/k2=v2 转为 k=v,k2=v2
  spec=$(echo "$part" | sed 's#/#, #g')

  loc=$(spark-sql --master local -e "DESC FORMATTED $table PARTITION ($spec)" 2>/dev/null | grep -i '^Location:' | awk '{print $2}')

  if [[ -z "$loc" ]]; then
    echo "Skip partition $part: cannot get location" >&2
    continue
  fi

  if [[ $loc != hdfs://DClusterUS1* ]]; then
    echo "Skip partition $part: location $loc does not start with hdfs://DClusterUS1" >&2
    continue
  fi

  # 判断库路径是否已经带 ns2
  if [[ $loc == $DB_LOC/ns2/* ]]; then
    rest=${loc#"$DB_LOC/"}
  else
    rest="ns2/${loc#"$DB_LOC/"}"
  fi

  new_loc="$NEW_DB_LOC/$rest"

  echo "ALTER TABLE $table PARTITION ($spec) SET LOCATION '$new_loc';"
done

