from datetime import date, datetime, timedelta, timezone
import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import pytest

from pydataflow_template.configs.config_factory import ConfigFactory
from pydataflow_template.configs.pipeline_config import PipelineConfigBuilder
from pydataflow_template.ios.ios import IoAdapter, IoType
from pydataflow_template.main import set_sink_result, set_source_result, set_transform_result
from pydataflow_template.modules.module_factory import ModuleFactory, ModuleProxy
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
            "parameters": {
                "query": "select id, name, date, memo from test.test;",
                "project": "py-dataflow",
            },
        }
    ],
    "sinks": [
        {
            "name": "out",
            "module": "bigquery",
            "input": "in",
            "parameters": {
                "table": "py-dataflow:test.test2",
                "create_disposition": "CREATE_IF_NEEDED",
            },
        }
    ],
}


PROFILE = {}

DATA = [
    {"id": 1, "name": "test data1", "date": date(2023, 1, 1), "memo": "memo1"},
    {"id": 2, "name": "test data2", "date": date(2023, 2, 2), "memo": None},
    {"id": 3, "name": "test data3", "date": date(2023, 3, 3), "memo": "memo3"},
    {"id": 4, "name": "test data4", "date": date(2023, 4, 4), "memo": None},
    {"id": 5, "name": "test data5", "date": date(2023, 5, 5), "memo": None},
]


class TestPostgresSourcePipeline:
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
        return PipelineConfigBuilder(
            ConfigFactory(IoAdapter(), gcs_temp_folder="gs://py-dataflow-bucket/integration_test/")
        )

    @pytest.fixture
    def module_proxy(self):
        return ModuleProxy(ModuleFactory())

    @pytest.fixture
    def options(self):
        beam_args = ["--runner=DirectRunner"]
        return beam.options.pipeline_options.PipelineOptions(
            beam_args, save_main_session=True, streaming=False
        )

    @pytest.mark.usefixtures("create_bigquery_test_table")
    def test_bigquery_to_bigquery_pipeline(
        self,
        base_config_json,
        base_profiles_json,
        io_adapter,
        config_builder,
        module_proxy,
    ):
        pipeline_config_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_config_json)
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        try:
            pipeline_config = config_builder.build(pipeline_config_dict, profiles_dict)
            num_modules = len(pipeline_config.get_module_names())
            executed_module_names = set()
            outputs = {}
            with TestPipeline() as pipeline:
                # expand graphs
                while len(executed_module_names) < num_modules:
                    set_source_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.sources,
                        outputs,
                        executed_module_names,
                    )
                    set_transform_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.transforms,
                        outputs,
                        executed_module_names,
                    )
                    set_sink_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.sinks,
                        outputs,
                        executed_module_names,
                    )

            assert_that(outputs["in"].pcol, equal_to(DATA))

            output_data = io_adapter.read(
                IoType.BIGQUERY_RDB_IO, query="select * from py-dataflow.test.test2"
            )
            assert sorted(output_data.to_dict("records"), key=lambda x: x["id"]) == DATA

        except Exception:
            pytest.fail()

    @pytest.mark.usefixtures("create_bigquery_test_table_dest")
    @pytest.mark.usefixtures("create_bigquery_test_table")
    def test_bigquery_to_bigquery_incremental_pipeline(
        self,
        base_config_json,
        base_profiles_json,
        io_adapter,
        config_builder,
        module_proxy,
        freezer,
    ):
        freezer.move_to(datetime(2023, 5, 6, 0, 0, tzinfo=JST))

        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "date",
                        "incremental_interval_from": "70day",
                        "destination_sink_name": "out",
                    },
                }
            ],
            "sinks": [
                {
                    "mode": "merge",
                    "parameters": {
                        "table": "py-dataflow:test.test_dest",
                        "merge_keys": ["date"],
                        "schema": "id:INTEGER,name:STRING,date:DATE,memo:STRING",
                        # "schema": "get_from_table",
                        "partitioning": "DAY",
                        "partitioning_field": "date",
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
            pipeline_config = config_builder.build(pipeline_config_dict, profiles_dict)
            num_modules = len(pipeline_config.get_module_names())
            executed_module_names = set()
            outputs = {}
            with TestPipeline() as pipeline:
                # expand graphs
                while len(executed_module_names) < num_modules:
                    set_source_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.sources,
                        outputs,
                        executed_module_names,
                    )
                    set_transform_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.transforms,
                        outputs,
                        executed_module_names,
                    )
                    set_sink_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.sinks,
                        outputs,
                        executed_module_names,
                    )

            # incremental from before 70days to 2023-05-06
            INCREMENTAL_DATA = [
                {"id": 3, "name": "test data3", "date": date(2023, 3, 3), "memo": "memo3"},
                {"id": 4, "name": "test data4", "date": date(2023, 4, 4), "memo": None},
                {"id": 5, "name": "test data5", "date": date(2023, 5, 5), "memo": None},
            ]

            assert_that(outputs["in"].pcol, equal_to(INCREMENTAL_DATA))

            output_data = io_adapter.read(
                IoType.BIGQUERY_RDB_IO, query="select * from py-dataflow.test.test_dest"
            )
            assert sorted(output_data.to_dict("records"), key=lambda x: x["id"]) == DATA

        except Exception:
            pytest.fail()

    @pytest.mark.usefixtures("create_bigquery_test_table_dest")
    @pytest.mark.usefixtures("create_bigquery_test_table")
    def test_bigquery_to_bigquery_incremental_pipeline2(
        self,
        base_config_json,
        base_profiles_json,
        io_adapter,
        config_builder,
        module_proxy,
        freezer,
    ):
        freezer.move_to(datetime(2023, 5, 6, 0, 0, tzinfo=JST))

        update_data = {
            "sources": [
                {
                    "incremental": True,
                    "parameters": {
                        "incremental_column": "date",
                        "incremental_interval_from": "70day",
                        "destination_sink_name": "out",
                    },
                }
            ],
            "sinks": [
                {
                    "mode": "merge",
                    "parameters": {
                        "table": "py-dataflow:test.test_dest",
                        "merge_keys": ["date"],
                        # "schema": "id:INTEGER,name:STRING,date:DATE,memo:STRING",
                        "schema": "get_from_table",
                        "partitioning": "DAY",
                        "partitioning_field": "date",
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
            pipeline_config = config_builder.build(pipeline_config_dict, profiles_dict)
            num_modules = len(pipeline_config.get_module_names())
            executed_module_names = set()
            outputs = {}
            with TestPipeline() as pipeline:
                # expand graphs
                while len(executed_module_names) < num_modules:
                    set_source_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.sources,
                        outputs,
                        executed_module_names,
                    )
                    set_transform_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.transforms,
                        outputs,
                        executed_module_names,
                    )
                    set_sink_result(
                        module_proxy,
                        pipeline,
                        pipeline_config.sinks,
                        outputs,
                        executed_module_names,
                    )

            # incremental from before 70days to 2023-05-06
            INCREMENTAL_DATA = [
                {"id": 3, "name": "test data3", "date": date(2023, 3, 3), "memo": "memo3"},
                {"id": 4, "name": "test data4", "date": date(2023, 4, 4), "memo": None},
                {"id": 5, "name": "test data5", "date": date(2023, 5, 5), "memo": None},
            ]

            assert_that(outputs["in"].pcol, equal_to(INCREMENTAL_DATA))

            output_data = io_adapter.read(
                IoType.BIGQUERY_RDB_IO, query="select * from py-dataflow.test.test_dest"
            )
            assert sorted(output_data.to_dict("records"), key=lambda x: x["id"]) == DATA

        except Exception:
            pytest.fail()
