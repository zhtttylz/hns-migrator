# hns-migrator

(hive ns 迁移工具) Hive NameService Migrator

## Scripts

The `scripts` directory contains small utilities used during HDFS
namespace migrations.

### `get_table_location.sh`
Query Hive for the location of a table using `spark-sql --master local`.

```bash
./scripts/get_table_location.sh <database.table>
```

### `show_hdfs_owners.sh`
Display the owner and group for every level of a given HDFS path.

```bash
./scripts/show_hdfs_owners.sh <hdfs-path>
```

### `get_table_info.sh`
Convenience wrapper that combines the above steps and prints the table
location and ownership information.

```bash
./scripts/get_table_info.sh <database.table>
```

Each script exits with an error message if required arguments are not
provided.
