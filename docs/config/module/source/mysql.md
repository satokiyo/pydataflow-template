# MySQL Source Module

The MySQL source module allows you to load data by specifying a query from a MySQL database.

## MySQL Source Module Parameters

The following parameters can be used to configure the MySQL source module:

| Parameter                 | Required (Default)                       | Type   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------- | ---------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| query                     | Required                                 | String | Specifies the SQL query used to read data from the MySQL database. You can also specify the path (gs://...) where the SQL file is located.                                                                                                                                                                                                                                                                                                       |
| profile [1]               | Required                                 | String | Specifies the key to identify the connection profile used in the connection profiles JSON file. It is also possible to directly configure the connection profile attributes in the configuration file by specifying the following attributes: `host`, `port`, `database`, `user`, `password`.                                                                                                                                                    |
| incremental_column        | Conditional                              | String | Required when the `incremental` parameter in the source module is set to `true`. Specifies the column name used as the basis for fetching incremental data. Note: The column should represent a time unit. Integer ranges are not supported.                                                                                                                                                                                                     |
| incremental_interval_from | Conditional (`max_value_in_destination`) | String | Required when the `incremental` parameter in the source module is set to `true`. Specifies the incremental interval. Use either `max_value_in_destination` (default) or specify the interval in the format `X[unit]` (X is an integer, and unit can be `min`, `hour`, or `day`). Example: 15min, 1hour, etc.                                                                                                                                     |
| destination_sink_name     | Conditional                              | String | Required when both the `incremental` parameter and `incremental_interval_from` parameter in the source module are set to `true`, and `incremental_interval_from` is set to `max_value_in_destination`. Specifies the name of the previous sink entry. The incremental data will be fetched from the values greater than `max(incremental_column)` in the specified table. Note: The name specified here must exist in the `sinks` configuration. |
| destination_search_range  | Optional                                 | String | Optional parameter used when the `incremental` parameter in the source module is set to `true` and `incremental_interval_from` is set to `max_value_in_destination`. Limits the range of the `max(incremental_column)` value from the previous sink table to reduce the scan size. Specify the range in the format `-X[unit]` (X is an integer, and unit can be `min`, `hour`, or `day`). Example: -15min, -1hour, etc.                          |

Please note that the `incremental` parameter mentioned above refers to a common parameter in the source module configuration.

The connection profiles JSON file mentioned in [1] should be specified as a JSON file using the `profile` parameter at startup. The file should contain entries with the necessary information in the following format (host, port, database, user, password):

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

You can either refer to these profiles by specifying the profile key or

directly configure the connection attributes in the module configuration.

## Example config files

- [mysql_to_bigquery](../../../../examples/mysql_to_bigquery.json)
