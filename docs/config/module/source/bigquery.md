# BigQuery Source Module

The BigQuery source module allows you to load data by specifying a query from BigQuery.

## BigQuery Source Module Parameters

The following parameters can be used to configure the BigQuery source module:

| Parameter                 | Required (Default)                       | Type   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------- | ---------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| query                     | Required                                 | String | Specifies the SQL query used to read data from BigQuery. You can also specify the path (gs://...) where the SQL file is located.                                                                                                                                                                                                                                                                                                                 |
| project                   | Required                                 | String | Specifies the project ID.                                                                                                                                                                                                                                                                                                                                                                                                                        |
| incremental_column        | Conditional                              | String | Required when the `incremental` parameter in the source module is set to `true`. Specifies the column name used as the basis for fetching incremental data. Note: The column should represent a time unit. Integer ranges are not supported.                                                                                                                                                                                                     |
| incremental_interval_from | Conditional (`max_value_in_destination`) | String | Required when the `incremental` parameter in the source module is set to `true`. Specifies the incremental interval. Use either `max_value_in_destination` (default) or specify the interval in the format `X[unit]` (X is an integer, and unit can be `min`, `hour`, or `day`). Example: 15min, 1hour, etc.                                                                                                                                     |
| destination_sink_name     | Conditional                              | String | Required when both the `incremental` parameter and `incremental_interval_from` parameter in the source module are set to `true`, and `incremental_interval_from` is set to `max_value_in_destination`. Specifies the name of the previous sink entry. The incremental data will be fetched from the values greater than `max(incremental_column)` in the specified table. Note: The name specified here must exist in the `sinks` configuration. |

Please note that the `incremental` parameter mentioned above refers to a common parameter in the source module configuration.

## Example config files

- [bigquery_to_bigquery](../../../../examples/bigquery_to_bigquery.json)
