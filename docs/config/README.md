# Define Pipeline

Define the configuration of a Dataflow pipeline in a JSON configuration file. See below for the syntax.

## Overview of JSON Configuration File

### Structure

Define a pipeline by combining three types of modules: `sources`, `transforms`, and `sinks`. Each module is a class implementing Apache Beam's Ptransform, which performs transformations on Pcollections.

The JSON configuration file consists of three sections: `sources`, `transforms`, and `sinks`. Each section contains entries that correspond to the configuration of the respective `sources`, `transforms`, and `sinks` modules. Multiple module configurations can be specified within each section, and they can be freely combined.

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

## Configuration Parameters

### Common Parameters for All Modules

Common parameters for all three module types: `sources`, `transforms`, and `sinks`.

| Parameter  | Required | Type                 | Description                                                                                      |
| ---------- | -------- | -------------------- | ------------------------------------------------------------------------------------------------ |
| name       | Required | String               | Specifies a unique name within the configuration file. It becomes the name of the Dataflow step. |
| module     | Required | String               | Specifies the module name. Refer to the [Module List](#module-list).                             |
| parameters | Required | Map<String, Object\> | Specifies the required parameters for each module. Refer to the [Module List](#module-list).     |

### Common Parameters for Source Modules

Common parameters for `sources` modules.

| Parameter   | Required (Default) | Type    | Description                                                                                     |
| ----------- | ------------------ | ------- | ----------------------------------------------------------------------------------------------- |
| incremental | Optional (`false`) | Boolean | If `false`, retrieves all data. If `true`, retrieves only incremental data. Default is `false`. |

### Common Parameters for Transform Modules

Common parameters for `transforms` modules.

| Parameter | Required (Default) | Type          | Description                                                                                                                           |
| --------- | ------------------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| inputs    | Required           | List<String\> | Specifies the name(s) (name parameter) of `sources` or `transforms` modules. The output of the specified module(s) becomes the input. |

### Common Parameters for Sink Modules

Common parameters for `sinks` modules.

| Parameter  | Required (Default)   | Type          | Description                                                                                                                      |
| ---------- | -------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| input      | Required             | String        | Specifies the name (name parameter) of a `sources` or `transforms` module. The output of the specified module becomes the input. |
| mode       | Optional (`replace`) | String        | Specifies one of the following: `replace`, `append`, `merge`. Default is `replace`.                                              |
| merge_keys | Conditional          | List<String\> | Required when mode=`merge`.                                                                                                      |

### Incremental Mode

The combinations of the `incremental` parameter in `sources` modules and the `mode` parameter in `sinks` modules are as follows:

|                        | `mode`==`replace` | `mode`==`append` | `mode`==`merge`  |
| ---------------------- | ----------------- | ---------------- | ---------------- |
| `incremental`==`false` | ✓                 | Validation Error | Validation Error |
| `incremental`==`true`  | Warning           | ✓[1]             | ✓[2]             |

[1] Only possible when the `incremental_interval_from` parameter in `sources` modules is set to `max_value_in_destination`. Otherwise, it results in a Validation Error.

[2] Merge implementation: TODO: Implementation using MERGE (DELETE + INSERT)

When the `merge_keys` parameter in `sources` modules is the same as the `incremental_column` parameter, the DELETE statement is as follows:

```sql
  DELETE FROM <TABLE> WHERE <incremental_column> > <incremental_interval_from>
```

When the `merge_keys` parameter is different from `incremental_column`, the DELETE statement is as follows:

```sql
  DELETE FROM <TABLE> WHERE (key1, key2, ...) IN (val1, val2, ...), (val1, val2, ...), ...
```

## Module List

### Source Modules

| Module                                | Full Load | Incremental Load | Streaming | Description                 |
| ------------------------------------- | --------- | ---------------- | --------- | --------------------------- |
| [bigquery](module/source/bigquery.md) | ✓         | ✓                | TODO      | Import data from BigQuery   |
| [mysql](module/source/mysql.md)       | ✓         | ✓                | TODO      | Import data from MySQL      |
| [postgres](module/source/postgres.md) | ✓         | ✓                | TODO      | Import data from PostgreSQL |

### Transform Modules

| Module                                 | Full Load | Incremental Load | Streaming | Description                          |
| -------------------------------------- | --------- | ---------------- | --------- | ------------------------------------ |
| [beamsql](module/transform/beamsql.md) | ✓         | -                | TODO      | Transform data by Beam SQL Transform |

[NOTE]
limitations: https://www.youtube.com/watch?v=zx4p-UNSmrA

- Available types are limited.
- Requires Java runtime

### Sink Modules

| Module                              | Full Load | Incremental Load | Streaming | Description                        |
| ----------------------------------- | --------- | ---------------- | --------- | ---------------------------------- |
| [bigquery](module/sink/bigquery.md) | ✓         | -                | ✓         | Inserting Data into BigQuery Table |

## Examples

Examples of JSON configuration files can be found in [examples/](../../examples/). Please refer to them.
