# Define Pipeline

Dataflow ジョブのパイプラインの設定を JSON 構成ファイルに定義する。書き方は下記参照。

## JSON 構成ファイル概要

### 構成

`sources`, `transforms`, `sinks`3 種類のモジュールを組み合わせることでパイプラインを定義する。各モジュールは、Apache Beam の Ptransform を実装したクラスであり、Pcollection の 変換処理を行う。

JSON 構成ファイルには `sources`, `transforms`, `sinks`の 3 種類のセクションがあり、各セクション内のエントリが、`sources`, `transforms`, `sinks` 3 種類のモジュールそれぞれに対応する設定を記述するエントリとなる。モジュールの設定は各セクション内に複数記述でき、また、自由に組み合わせ可能。

```json
{
  "sources": [
    {...}
  ],
  "transforms": [
    {...}
  ],
  "sinks": [
    {...}
  ]
}
```

## 設定パラメーター

### 全 modules 共通パラメーター

`sources`, `transforms`, `sinks`3 種類の全てのモジュールの共通パラメーター

| parameter  | required | type                 | description                                                                          |
| ---------- | -------- | -------------------- | ------------------------------------------------------------------------------------ |
| name       | Required | String               | 設定ファイル内で一意の名前を指定する。Dataflow step の名前になる。                   |
| module     | Required | String               | モジュール名を指定する。[モジュール一覧](#モジュール一覧) を参照                     |
| parameters | Required | Map<String, Object\> | 各モジュールで必要なパラメーターを指定する。[モジュール一覧](#モジュール一覧) を参照 |

### Source modules 共通パラメーター

`sources`モジュールの共通パラメーター

| parameter   | required (default) | type    | description                                                                             |
| ----------- | ------------------ | ------- | --------------------------------------------------------------------------------------- |
| incremental | Optional (`false`) | Boolean | `false` の場合、全量データを取得。`true` の場合、差分データを取得。デフォルトは `false` |

### Transform modules 共通パラメーター

`transforms`モジュールの共通パラメーター

| parameter | required (default) | type          | description                                                                                           |
| --------- | ------------------ | ------------- | ----------------------------------------------------------------------------------------------------- |
| inputs    | Required           | List<String\> | `sources` または `transforms`モジュールの名前(name)を指定する。指定したモジュールの出力が入力となる。 |

### Sink modules 共通パラメーター

`sinks`モジュールの共通パラメーター

| parameter  | required (default)   | type          | description                                                                                           |
| ---------- | -------------------- | ------------- | ----------------------------------------------------------------------------------------------------- |
| input      | Required             | String        | `sources` または `transforms`モジュールの名前(name)を指定する。指定したモジュールの出力が入力となる。 |
| mode       | Optional (`replace`) | String        | `replace`, `append`, `merge` のいずれかを指定する。デフォルトは`replace`                              |
| merge_keys | Conditional          | List<String\> | mode=`merge`のとき指定必須。                                                                          |

### incremental モードについて

`sources`モジュールの共通パラメーター `incremental`と、`sinks`モジュールの`mode`の設定の組み合わせ可否は下表の通り。

|                        | `mode`==`replace` | `mode`==`append` | `mode`==`merge`  |
| ---------------------- | ----------------- | ---------------- | ---------------- |
| `incremental`==`false` | 〇                | Validation Error | Validation Error |
| `incremental`==`true`  | Warning           | 〇[1]            | 〇[2]            |

[1] `sources`モジュールのパラメーター `incremental_interval_from` == `max_value_in_destination`の場合のみ可能。それ以外は Validation Error。

[2] Merge の実装は DELETE+INSERT による(TODO：MERGE による実装)

`sources`モジュールのパラメーター `merge_keys`が、`sources`モジュールのパラメーター `incremental_column`と同じ場合の DELETE 文は以下のようになる

```sql
  DELETE FROM <TABLE> WHERE <incremental_column> > <incremental_interval_from>
```

`merge_keys`が`incremental_column`と異なる場合の DELETE 文は以下のようになる

```sql
  DELETE FROM <TABLE> WHERE (key1, key2, ...) IN (val1, val2, ...), (val1, val2, ...), ...
```

## モジュール一覧

### Source Modules

| module                                   | full load | incremental load | streaming | description                 |
| ---------------------------------------- | --------- | ---------------- | --------- | --------------------------- |
| [bigquery](module/source/bigquery_ja.md) | ○         | ○                | TODO      | Import data from BigQuery   |
| [mysql](module/source/mysql_ja.md)       | ○         | ○                | TODO      | Import data from MySQL      |
| [postgres](module/source/postgres_ja.md) | ○         | ○                | TODO      | Import data from PostgreSQL |

### Transform Modules

| module                                    | full load | incremental load | streaming | description                          |
| ----------------------------------------- | --------- | ---------------- | --------- | ------------------------------------ |
| [beamsql](module/transform/beamsql_ja.md) | ○         | -                | TODO      | Transform data by Beam SQL Transform |

[NOTE]
limitations: https://www.youtube.com/watch?v=zx4p-UNSmrA

- Available types are limited.
- Requires Java runtime

### Sink Modules

| module                                 | full load | incremental load | streaming | description                        |
| -------------------------------------- | --------- | ---------------- | --------- | ---------------------------------- |
| [bigquery](module/sink/bigquery_ja.md) | ○         | -                | ○         | Inserting Data into BigQuery Table |

## Examples

JSON 構成ファイルの例は [examples/](../../examples/) にあるので、参考にしてください。
