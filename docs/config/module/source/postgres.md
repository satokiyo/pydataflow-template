# PostgreSQL Source Module

The PostgreSQL source module allows you to load data by specifying a query from a PostgreSQL database.

## PostgreSQL Source Module Parameters

The following parameters can be used to configure the PostgreSQL source module:

| Parameter                 | Required (Default)                       | Type   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------- | ---------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| query [1]                 | Required                                 | String | Specifies the SQL query used to read data from the PostgreSQL database. You can also specify the path (gs://...) where the SQL file is located.                                                                                                                                                                                                                                                                                                  |
| profile [2]               | Required                                 | String | Specifies the key to identify the connection profile used in the connection profiles JSON file. It is also possible to directly configure the connection profile attributes in the configuration file by specifying the following attributes: `host`, `port`, `database`, `user`, `password`.                                                                                                                                                    |
| incremental_column        | Conditional                              | String | Required when the `incremental` parameter in the source module is set to `true`. Specifies the column name used as the basis for fetching incremental data. Note: The column should represent a time unit. Integer ranges are not supported.                                                                                                                                                                                                     |
| incremental_interval_from | Conditional (`max_value_in_destination`) | String | Required when the `incremental` parameter in the source module is set to `true`. Specifies the incremental interval. Use either `max_value_in_destination` (default) or specify the interval in the format `X[unit]` (X is an integer, and unit can be `min`, `hour`, or `day`). Example: 15min, 1hour, etc.                                                                                                                                     |
| destination_sink_name     | Conditional                              | String | Required when both the `incremental` parameter and `incremental_interval_from` parameter in the source module are set to `true`, and `incremental_interval_from` is set to `max_value_in_destination`. Specifies the name of the previous sink entry. The incremental data will be fetched from the values greater than `max(incremental_column)` in the specified table. Note: The name specified here must exist in the `sinks` configuration. |
| destination_search_range  | Optional                                 | String | Optional parameter used when the `incremental` parameter in the source module is set to `true` and `incremental_interval_from` is set to `max_value_in_destination`. Limits the range of the `max(incremental_column)` value from the previous sink table to reduce the scan size. Specify the range in the format `-X[unit]` (X is an integer, and unit can be `min`, `hour`, or `day`). Example: -15min, -1hour, etc.                          |

Please note the following additional information:

1. The Postgres source module supports query splitting in the connector. You can split SQL queries by using the separator (`--sep--`) to process multiple queries in parallel during Dataflow job execution. Here's an example:

```sql
/* Example */

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

2. The connection profiles JSON file mentioned in [2] should be specified as a JSON file using the `profile` parameter at startup. The file should contain entries with the necessary information in the following format (host, port, database, user, password):

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

Please make sure to replace the example values with the actual connection details for your PostgreSQL database.
