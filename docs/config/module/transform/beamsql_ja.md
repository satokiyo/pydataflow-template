# BeamSQL Transform Module

Transform Module for transforming data by Beam SQL.

## BeamSQL transform module parameters

| parameter    | required (default) | type          | description                                            |
| ------------ | ------------------ | ------------- | ------------------------------------------------------ |
| sql [1]      | Required           | String        | Specify the SQL to perform transformation of the data. |
| inputs_alias | Optional           | List<String\> | Specify the aliases for inputs in sql.                 |

[1] SQL 文で使用できるカラムのデータ型には制約がある。また、Java の Runtime が必要になる。詳細は Apache beam python のドキュメント参照
