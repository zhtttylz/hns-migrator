# hns-migrator

*(Hive NameService Migrator)*

```text
 _   _ _  _ ____   __  __ _
| | | | || |  _ \ / _|/ _(_)_ __   ___
| |_| | || | | | | |_| |_| | '_ \ / _ \
|  _  |__   _| |_| |  _|  _| | | | |  __/
|_| |_|   |_| |____/|_| |_| |_| |_|\___|
```

> HDFS 名称迁移工具，为 Hive NameService 迁移封装的小脚本集合。

## 脚本一览

`us_scripts` 目录中包含五个小工具，以下为它们的功能简介和使用示例。

### 1. `get_table_location.sh`
- 功能：通过 `spark-sql` 查询给定 Hive 表的 Location。
- 使用：
  ```bash
  ./us_scripts/get_table_location.sh sample_db.sample_table
  ```
- 示例输出：
  ```
  hdfs://DClusterUS1/user/hive/warehouse/sample_db.db/sample_table
  ```

### 2. `show_hdfs_owners.sh`
- 功能：递归显示指定 HDFS 路径各级目录的 owner 和 group，并将信息记录到 `hdfs_owners.log`。
- 使用：
  ```bash
  ./us_scripts/show_hdfs_owners.sh hdfs://DClusterUS1/user/hive/warehouse/sample_db.db/sample_table
  ```
- 示例输出：
  ```
  hdfs://DClusterUS1/user/bigdata/hive owner hive
  hdfs://DClusterUS1/user/bigdata/warehouse owner hive
  ...
  ```

### 3. `get_table_info.sh`
- 功能：结合上述两个脚本，先获取表路径，再显示其各层目录的拥有者信息。
- 使用：
  ```bash
  ./us_scripts/get_table_info.sh sample_db.sample_table
  ```
- 示例输出：
  ```
  Table location: hdfs://DClusterUS1/user/hive/warehouse/sample_db.db/sample_table
  hdfs://DClusterUS1/user/hive owner hive
  hdfs://DClusterUS1/user/hive/warehouse owner hive
  ...
  ```

### 4. `verify_paths.sh`
- 功能：根据 `hdfs_owners.log` 生成在 DClusterUS2 创建目录的 `hadoop fs` 命令。
- 使用：
  ```bash
  ./us_scripts/verify_paths.sh
  ```
- 示例输出：
  ```
  ====================================================
  hadoop fs -mkdir hdfs://DClusterUS2/user/bigdata/hive
  hadoop fs -chown owner:hive hdfs://DClusterUS2/user/bigdata/hive
  ...
  ```

### 5. `gen_part_set_location.sh`
- 功能：为指定分区表生成分区 `SET LOCATION` SQL 语句，将路径从 `DClusterUS1` 调整到 `DClusterUS2`。
- 使用：
  ```bash
  ./us_scripts/gen_part_set_location.sh sample_db.sample_table
  ```
- 示例输出：
  ```
  总共 3 个分区
  扫描分区dt='20240101'对应的分区路径
  dt='20240101'的分区路径为hdfs://DClusterUS1/.../dt=20240101
  ALTER TABLE sample_db.sample_table PARTITION (dt='20240101') SET LOCATION 'hdfs://DClusterUS2/.../ns2table_name/dt=20240101'
  ...
  ```

每个脚本都会在参数缺失时给出错误信息并退出。
