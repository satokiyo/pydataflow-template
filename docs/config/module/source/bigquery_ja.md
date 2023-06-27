# BigQuery Source Module

Source Module for loading data by specifying a query from BigQuery.

## BigQuery source module parameters

| parameter                 | required (default)                       | type   | description                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------- | ---------------------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| query                     | Required                                 | String | Specify the SQL to read data from BigQuery. You can also specify the path (gs://...) where you put the SQL file.                                                                                                                                                                                                                                                        |
| project                   | Required                                 | String | Specify the project id.                                                                                                                                                                                                                                                                                                                                                 |
| incremental_column        | Conditional                              | String | Source モジュール共通パラメーターの `incremental` == `true`のとき指定必須。 incremental データを取得する基準となるカラム名を指定する。※ 時間単位の列にする。整数範囲などは未対応                                                                                                                                                                                        |
| incremental_interval_from | Conditional (`max_value_in_destination`) | String | Source モジュール共通パラメーターの `incremental` == ` true`のとき指定必須。 incremental の interval を指定する。 `max_value_in_destination`(default) または `X[unit]` 形式で指定（X は整数、unit は[`min`, `hour`, `day`]のいずれか)。 example: 15min, 1hour, ... etc.                                                                                                 |
| destination_sink_name     | Conditional                              | String | Source モジュール共通パラメーターの `incremental` == `true` かつ `incremental_interval_from` == `max_value_in_destination` のとき指定必須。前回連携時の sink のエントリの name を指定する。ここに指定されたテーブルの`max(incremental_column)`の以降の値を incremental データとして取得する。 ※ `sinks`に存在しない name は指定できない。                               |
| destination_search_range  | Optional                                 | String | Source モジュール共通パラメーターの `incremental` == `true` かつ `incremental_interval_from` == `max_value_in_destination` のときオプション指定可。前回連携時の sink テーブルの`max(incremental_column)`の値を取得する範囲を制限して、スキャン量を抑える。`-X[unit]` 形式で指定（X は整数、unit は[`min`, `hour`, `day`]のいずれか)。 example: -15min, -1hour, ... etc. |

## 構成例

- [bigquery_to_bigquery](../../../../examples/bigquery_to_bigquery.json)
