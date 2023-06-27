# Postgres Source Module

Source Module for loading data by specifying a query from PostgreSQL.

## Postgres source module parameters

| parameter                 | required (default)                       | type   | description                                                                                                                                                                                                                                                                                                                                                              |
| ------------------------- | ---------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| query [1]                 | Required                                 | String | Specify the SQL to read data from BigQuery. You can also specify the path (gs://...) where you put the SQL file.                                                                                                                                                                                                                                                         |
| profile [2]               | Required                                 | String | Specify a key to specify the connection profile you use in the connection profiles json file. it is also possible to directly configure connection profile attributes in the config file by specifing the following attributes: `host`, `port`, `database`, `user`, `password`.                                                                                          |
| incremental_column        | Conditional                              | String | Source モジュール共通パラメーターの `incremental` == `true`のとき指定必須。 incremental データを取得する基準となるカラム名を指定する。※ 時間単位の列にする。整数範囲などは未対応                                                                                                                                                                                         |
| incremental_interval_from | Conditional (`max_value_in_destination`) | String | Source モジュール共通パラメーターの `incremental` == ` true`のとき指定必須。 incremental の interval を指定する。 `max_value_in_destination`(default) または `X[unit]` 形式で指定（X は整数、unit は[`min`, `hour`, `day`]のいずれか)。 example: 15min, 1hour, ... etc.                                                                                                  |
| destination_sink_name     | Conditional                              | String | Source モジュール共通パラメーターの `incremental` == ` true` かつ `incremental_interval_from` == `max_value_in_destination` のとき指定必須。前回連携時の sink のエントリの name を指定する。ここに指定されたテーブルの`max(incremental_column)`の以降の値を incremental データとして取得する。 ※ `sinks`に存在しない name は指定できない。                               |
| destination_search_range  | Optional                                 | String | Source モジュール共通パラメーターの `incremental` == ` true` かつ `incremental_interval_from` == `max_value_in_destination` のときオプション指定可。前回連携時の sink テーブルの`max(incremental_column)`の値を取得する範囲を制限して、スキャン量を抑える。`-X[unit]` 形式で指定（X は整数、unit は[`min`, `hour`, `day`]のいずれか)。 example: -15min, -1hour, ... etc. |

[1] Postgres source module では、コネクターで query の分割に対応している。sql をセパレーター(`--sep--`)で区切って複数に分割することで、Dataflow でのジョブ実行時に並列でクエリを処理する

```sql
/* 例 */

SELECT
    ...
FROM xxx_table
WHERE created_at <= '2021-12-31';

--sep--

SELECT
    ...
FROM xxx_table
WHERE created_at BETWEEN '2022-01-01' AND '2022-12-31';

--sep--

SELECT
    ...
FROM xxx_table
WHERE created_at >= '2023-01-01';
```

[2] connection profiles json は 起動時パラメーター profile として指定する JSON ファイル。指定したエントリに必要な情報を以下のような形式で記載していること(host, port, database, user, password)。

```json:connections.json
{
  "profile_key1": {
    "host": "0.0.0.0",
    "port": 5432,
    "database": "test_db",
    "user": "user",
    "password": "passw0rd"
  },
  "profile_key2": {
    "host": "0.0.0.0",
    "port": 3306,
    "database": "test_db",
    "user": "user",
    "password": "passw0rd"
  }
}
```

## 構成例

- [postgres_to_bigquery](../../../../examples/postgres_to_bigquery.json)
- [postgres_to_bigquery_incremental_merge](../../../../examples/postgres_to_bigquery_incremental_merge.json)
