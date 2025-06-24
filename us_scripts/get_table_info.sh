#!/bin/bash
#开启“严格模式”
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <database.table>" >&2
  exit 1
fi

table=$1

location=$("$(dirname "$0")/get_table_location.sh" "$table")

if [ -z "$location" ]; then
  echo "Failed to retrieve location for table $table" >&2
  exit 1
fi

echo "Table location: $location"

"$(dirname "$0")/show_hdfs_owners.sh" "$location"
