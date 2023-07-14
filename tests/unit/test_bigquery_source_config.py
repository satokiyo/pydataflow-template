from datetime import datetime, timedelta, timezone
import json
from logging import INFO

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
            "module": "bigquery",
            "incremental": False,
            "parameters": {"query": "select id from test;", "project": "my_project"},
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
            },
        }
    ],
}

PROFILE = {}


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


class TestBigQuerySourceConfig:
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
            {"sources": [{"name": ""}]},
            {"sources": [{"name": None}]},
            {"sources": [{"module": ""}]},
            {"sources": [{"module": None}]},
            {"sources": [{"incremental": "Unsupported mode"}]},
            {"sources": [{"parameters": {"query": ""}}]},
            {"sources": [{"parameters": {"query": None}}]},
            {"sources": [{"parameters": {"project": ""}}]},
            {"sources": [{"parameters": {"project": None}}]},
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

    # incremental test
    def test_invalid_incremental_config(
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
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        config_builder.build(pipeline_config_dict, profiles_dict)

    def test_incremental_interval_without_destination_sink_name(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "incremental_interval_from": "1day",
                        "destination_sink_name": None,
                        "incremental_column": "day",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        config_builder.build(pipeline_config_dict, profiles_dict)

    def test_incremental_column_is_not_specified_in_incremental_mode(
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
                    "incremental": True,
                    "parameters": {
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_sink_name": "out-test",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        caplog.set_level(INFO)
        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_incremental_interval_from_is_not_specified_in_incremental_mode(
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
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "incremental_column",
                        "incremental_interval_from": None,
                        "destination_sink_name": "out-test",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        caplog.set_level(INFO)
        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_destination_sink_name_is_not_specified_when_incremental_interval_from_is_max_value_in_destination(
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
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "incremental_column",
                        "incremental_interval_from": "max_value_in_destination",
                    },
                }
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        caplog.set_level(INFO)
        with pytest.raises(Exception) as e:
            config_builder.build(pipeline_config_dict, profiles_dict)

        assert str(e.typename) == "ParameterValidationError"

    def test_incremental_interval_from_overwrite_source_query_1(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, time from test.test;",
                        "incremental_interval_from": "10MIN",
                        "incremental_column": "time",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,time:DATETIME",
                        "partitioning_field": "time",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(time AS DATETIME) > CAST('2022-12-31 23:50:00' AS DATETIME)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_overwrite_source_query_2(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, time from test.test;",
                        "incremental_interval_from": "10min",
                        "incremental_column": "time",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,time:DATETIME",
                        "partitioning_field": "time",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(time AS DATETIME) > CAST('2022-12-31 23:50:00' AS DATETIME)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_overwrite_source_query_3(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, time from test.test;",
                        "incremental_interval_from": "24hour",
                        "incremental_column": "time",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,time:DATETIME",
                        "partitioning_field": "time",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(time AS DATETIME) > CAST('2022-12-31 00:00:00' AS DATETIME)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_overwrite_source_query_4(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, time from test.test;",
                        "incremental_interval_from": "24hour",
                        "incremental_column": "time",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,time:TIMESTAMP",
                        "partitioning_field": "time",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(time AS DATETIME) > CAST('2022-12-31 00:00:00' AS DATETIME)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_overwrite_source_query_5(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day from test.test;",
                        "incremental_interval_from": "20day",
                        "incremental_column": "day",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,day:DATE",
                        "partitioning_field": "day",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(day AS DATE) > CAST('2022-12-12' AS DATE)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_overwrite_source_query_6(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, time from test.test;",
                        "incremental_interval_from": "10min",
                        "incremental_column": "time",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,time:DATETIME",
                        "partitioning_field": "time",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(time AS DATETIME) > CAST('2022-12-31 23:50:00' AS DATETIME)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_overwrite_source_query_7(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, time from test.test;",
                        "incremental_interval_from": "10hour",
                        "incremental_column": "time",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,time:DATETIME",
                        "partitioning_field": "time",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(time AS DATETIME) > CAST('2022-12-31 14:00:00' AS DATETIME)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_overwrite_source_query_8(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, time from test.test;",
                        "incremental_interval_from": "10hour",
                        "incremental_column": "time",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,time:DATETIME",
                        "partitioning_field": "time",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(time AS DATETIME) > CAST('2022-12-31 14:00:00' AS DATETIME)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_overwrite_source_query_9(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
        freezer,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, time from test.test;",
                        "incremental_interval_from": "10day",
                        "incremental_column": "time",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "schema": "id:INTEGER,time:DATETIME",
                        "partitioning_field": "time",
                    }
                },
            ],
        }
        pipeline_config_file_path = update_config_json(update_data, base_config_json)

        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        freezer.move_to(datetime(2023, 1, 1, 0, 0, tzinfo=JST))

        expected = "CAST(time AS DATE) > CAST('2022-12-22' AS DATE)"

        config = config_builder.build(pipeline_config_dict, profiles_dict)

        assert expected in config.sources[0].query.sql

    def test_incremental_interval_from_format_invalid1(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day, datetime, timestamp from test.test;",
                        "incremental_interval_from": "-1hour",
                        "incremental_column": "timestamp",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "partitioning_field": "timestamp",
                        "schema": "id:INTEGER,day:DATE,datetime:DATETIME,timestamp:TIMESTAMP",
                    }
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

    def test_incremental_interval_from_format_invalid2(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day, datetime, timestamp from test.test;",
                        "incremental_interval_from": "1 hour",
                        "incremental_column": "timestamp",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "partitioning_field": "timestamp",
                        "schema": "id:INTEGER,day:DATE,datetime:DATETIME,timestamp:TIMESTAMP",
                    }
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

    def test_destination_search_range_format_invalid1(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day, datetime, timestamp from test.test;",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_search_range": "-1 hour",
                        "incremental_column": "timestamp",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "partitioning_field": "timestamp",
                        "schema": "id:INTEGER,day:DATE,datetime:DATETIME,timestamp:TIMESTAMP",
                    }
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

    def test_destination_search_range_format_invalid2(
        self,
        base_config_json,
        base_profiles_json,
        config_builder,
        io_adapter,
    ):
        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "query": "select id, day, datetime, timestamp from test.test;",
                        "incremental_interval_from": "max_value_in_destination",
                        "destination_search_range": "1hour",
                        "incremental_column": "timestamp",
                    },
                },
            ],
            "sinks": [
                {
                    "parameters": {
                        "partitioning_field": "timestamp",
                        "schema": "id:INTEGER,day:DATE,datetime:DATETIME,timestamp:TIMESTAMP",
                    }
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
