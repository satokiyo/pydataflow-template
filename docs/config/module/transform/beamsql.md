# BeamSQL Transform Module

This is the README.md for the BeamSQL Transform Module, which allows data transformation using Beam SQL.

## BeamSQL transform module parameters

| Parameter    | Required (Default) | Type          | Description                                     |
| ------------ | ------------------ | ------------- | ----------------------------------------------- |
| sql [1]      | Required           | String        | Specify the SQL to perform data transformation. |
| inputs_alias | Optional           | List<String\> | Specify the aliases for inputs in the SQL.      |

[1] There are constraints on the data types of columns that can be used in the SQL statement. Additionally, Java Runtime is required. For more details, refer to the documentation of Apache Beam Python.

## Example config files

- [beamsql](../../../../examples/beamsql.json)
