#!/bin/bash

#开启“严格模式”
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <database.table>" >&2
  exit 1
fi

table=$1


location=$(spark-sql --master local -e "DESC FORMATTED $table" 2>/dev/null \
           | grep -i '^Location:' | awk '{print $2}')

echo "$location"
