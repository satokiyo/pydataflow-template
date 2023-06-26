from datetime import timedelta, timezone
import json
from logging import DEBUG

from _pytest.logging import LogCaptureFixture
import pandas as pd
import pytest

from pydataflow_template.configs.config_factory import ConfigFactory
from pydataflow_template.configs.pipeline_config import PipelineConfigBuilder
from pydataflow_template.ios.ios import IoAdapter, IoType
from pydataflow_template.ios.schema import MetaData, Schema
from tests.helper.utils import update_config_json

JST = timezone(timedelta(hours=+9), "JST")

# config base template
BASE_CONFIG = {
    "name": "test",
    "description": "test base",
    "sources": [
        {
            "name": "in",
            "module": "postgres",
            "incremental": False,
            "parameters": {
                "query": "select id from test;",
                "profile": "test",
            },
        }
    ],
    "sinks": [
        {
            "name": "out",
            "module": "bigquery",
            "input": "in",
            "mode": "replace",
            "parameters": {
                "table": "my-prj:test.test",
                "schema": "id:INTEGER,day:DATE",
                "partitioning": "DAY",
                "partitioning_field": "day",
                "clustering": "id",
            },
        }
    ],
}

PROFILE = {
    "test": {
        "host": "host",
        "port": 5432,
        "database": "database",
        "user": "user",
        "password": "password",
    },
}


class MockIoAdapter:
    def read(self, *args, **kwargs):
        MOCK_PREV_MAX = "2022-01-01"
        return pd.DataFrame([MOCK_PREV_MAX])

    def get_metadata(self, *args, **kwargs):
        return MetaData(
            schema=Schema().from_dict(
                [
                    {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "day", "type": "DATE", "mode": "NULLABLE"},
                    {"name": "datetime", "type": "DATETIME", "mode": "NULLABLE"},
                    {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
                ],
                src_system="bigquery",
            ),
            row_count=1,
        )


class TestBigQuerySinkConfig:
    @pytest.fixture(scope="function")
    def base_config_json(self, tmp_path):
        file_path = tmp_path / "config_base.json"
        file_path.write_text(json.dumps(BASE_CONFIG))
        return str(file_path)

    @pytest.fixture(scope="function")
    def base_profiles_json(self, tmp_path):
        file_path = tmp_path / "profiles.json"
        file_path.write_text(json.dumps(PROFILE))
        return str(file_path)

    @pytest.fixture
    def io_adapter(self):
        return IoAdapter()

    @pytest.fixture
    def config_builder(self):
        return PipelineConfigBuilder(ConfigFactory(MockIoAdapter(), gcs_temp_folder="gs://test/"))

    def test_default_config_json(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        pipeline_config_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_config_json)
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        _ = config_builder.build(pipeline_config_dict, profiles_dict)

    @pytest.mark.parametrize(
        "update_data",
        [
            {"sinks": [{"name": ""}]},
            {"sinks": [{"name": None}]},
            {"sinks": [{"module": ""}]},
            {"sinks": [{"module": None}]},
            {"sinks": [{"input": ""}]},
            {"sinks": [{"input": None}]},
            {"sinks": [{"mode": ""}]},
            {"sinks": [{"mode": None}]},
            {"sinks": [{"parameters": {"table": ""}}]},
            {"sinks": [{"parameters": {"table": None}}]},
        ],
    )
    def test_invalid_parameter(
        self, update_data, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception):
            config_builder.build(pipeline_config_dict, profiles_dict)

    def test_mode_of_the_sink_config_must_be_replace_when_mode_of_source_config_is_not_incremental(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "mode": "append",
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_table_format_is_invalid(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {"table": "project.dataset.table"},
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_schema_format_invalid(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {
                        "schema": "id INTEGER",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_partitioning_invalid(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {
                        "partitioning": "MINUTES",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_partitioning_is_specified_but_partitioning_field_is_not_specified(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": None,
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_partitioning_field_not_in_target_table_schema(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP",
                        "partitioning": "DAY",
                        "partitioning_field": "foo",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_partitioning_field_type_invalid(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP,memo:STRING",
                        "partitioning": "DAY",
                        "partitioning_field": "memo",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_clustering_format_invalid_1(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP,memo1:STRING,memo2:STRING,memo3:STRING",
                        "clustering": "day time",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_clustering_format_invalid_2(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP,memo1:STRING,memo2:STRING,memo3:STRING",
                        "clustering": "day,",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_too_many_clustering_columns(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP,memo1:STRING,memo2:STRING,memo3:STRING",
                        "clustering": "day,time,memo1,memo2,memo3",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    # incremental test
    def test_incremental_valid(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "day",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                    },
                }
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP,memo1:STRING,memo2:STRING,memo3:STRING",
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                        "create_disposition": "CREATE_NEVER",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        try:
            config_builder.build(pipeline_config_dict, profiles_dict)
        except Exception:
            pytest.fail()

    def test_incremental_multiple_source_invalid(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "day",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP,memo1:STRING,memo2:STRING,memo3:STRING",
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                        "create_disposition": "CREATE_NEVER",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        # multiple input
        input2 = pipeline_config_dict["sources"][0].copy()
        input2["name"] = "in2"
        pipeline_config_dict["sources"].append(input2)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_incremental_invalid_partitioning_field(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "day",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP,memo1:STRING,memo2:STRING,memo3:STRING",
                        "partitioning": "DAY",
                        "partitioning_field": "",
                        "create_disposition": "CREATE_NEVER",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_incremental_invalid_partitioning(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "day",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP,memo1:STRING,memo2:STRING,memo3:STRING",
                        "partitioning_field": "",
                        "create_disposition": "CREATE_NEVER",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_cannot_be_specified_create_if_needed_in_incremental_mode(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "day",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                        "create_disposition": "CREATE_IF_NEEDED",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_incremental_column_is_not_in_target_sink_schema_in_incremental_mode(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day from test.test;",
                        "incremental_column": "day",
                        "incremental_interval_from": "20day",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "partitioning": "DAY",
                        "schema": "id:INTEGER,day:DATE",
                        "partitioning_field": "time",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_incremental_column_is_not_in_target_sink_schema_when_incremental_interval_from_is_max_value_in_destination_in_incremental_mode(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day from test.test;",
                        "incremental_column": "time",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "time",
                        "schema": "id:INTEGER,day:DATE",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_get_incremental_interval_from_destination_table(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        caplog: LogCaptureFixture,
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day from test.test;",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                        "incremental_column": "day",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        expected = "select max(day) from test.test"

        caplog.set_level(DEBUG)
        config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in caplog.text.lower()

    def test_get_incremental_interval_from_destination_table_with_specified_search_range_from_date_type_field(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        caplog: LogCaptureFixture,
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day, datetime, timestamp from test.test;",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                        "destination_search_range": "-1hour",
                        "incremental_column": "day",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                        "schema": "id:INTEGER,day:DATE,datetime:DATETIME,timestamp:TIMESTAMP",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        expected = "select max(day) from test.test where day >= date(timestamp_add(current_timestamp(), interval -1 hour)"

        caplog.set_level(DEBUG)
        config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in caplog.text.lower()

    def test_get_incremental_interval_from_destination_table_with_specified_search_range_from_datetime_type_field(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        caplog: LogCaptureFixture,
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "module": "mysql",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day, datetime, timestamp from test.test;",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                        "destination_search_range": "-1hour",
                        "incremental_column": "datetime",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "datetime",
                        "schema": "id:INTEGER,day:DATE,datetime:DATETIME,timestamp:TIMESTAMP",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        expected = "select max(datetime) from test.test where datetime >= datetime(timestamp_add(current_timestamp(), interval -1 hour)"

        caplog.set_level(DEBUG)
        config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in caplog.text.lower()

    def test_get_incremental_interval_from_destination_table_with_specified_search_range_from_timestamp_type_field(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        caplog: LogCaptureFixture,
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "module": "mysql",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day, datetime, timestamp from test.test;",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                        "destination_search_range": "-1hour",
                        "incremental_column": "timestamp",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "timestamp",
                        "schema": "id:INTEGER,day:DATE,datetime:DATETIME,timestamp:TIMESTAMP",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        expected = "select max(timestamp) from test.test where timestamp >= timestamp(timestamp_add(current_timestamp(), interval -1 hour)"

        caplog.set_level(DEBUG)
        config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in caplog.text.lower()

    def test_mode_of_sink_config_expected_to_be_append_or_merge_in_incremental_mode(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        caplog: LogCaptureFixture,
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "day",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "replace",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        expected = "The mode of the sink configuration is expected to be 'append' or 'merge' in incremental mode, but 'replace' is specified."

        caplog.set_level(DEBUG)
        config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected.lower() in caplog.text.lower()

    def test_merge_keys_is_not_specified_in_incremental_merge(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day from test.test;",
                        "incremental_column": "day",
                        "incremental_interval_from": "10day",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "merge",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_append_mode_of_the_sink_config_is_allowed_only_when_incremental_interval_from_of_source_config_is_max_value_in_destination(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day from test.test;",
                        "incremental_column": "day",
                        "incremental_interval_from": "10day",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_merge_keys_is_specified_but_the_mode_is_not_merge(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "name": "in",
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day from test.test;",
                        "incremental_column": "day",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out",
                    },
                },
            ],
            "sinks": [
                {
                    "mode": "append",
                    "parameters": {
                        "merge_keys": ["id", "day"],
                        "partitioning": "DAY",
                        "partitioning_field": "day",
                        "schema": "id:INTEGER,day:DATE,time:TIMESTAMP",
                    },
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"
