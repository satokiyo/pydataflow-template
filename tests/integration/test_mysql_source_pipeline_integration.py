from datetime import date, timedelta, timezone
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

JST = timezone(timedelta(hours=+9), "JST")

# config base template
BASE_CONFIG = {
    "name": "test",
    "description": "test base",
    "sources": [
        {
            "name": "in",
            "module": "mysql",
            "incremental": False,
            "parameters": {
                "query": "select id, name, date, memo from test;",
                "profile": "test",
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
            },
        }
    ],
}

PROFILE = {
    "test": {
        "host": "localhost",
        "port": 3306,
        "database": "test_db",
        "user": "user",
        "password": "passw0rd",
    },
}
DATA = [
    {"id": 1, "name": "test data1", "date": date(2023, 1, 1), "memo": "memo1"},
    {"id": 2, "name": "test data2", "date": date(2023, 2, 2), "memo": None},
    {"id": 3, "name": "test data3", "date": date(2023, 3, 3), "memo": "memo3"},
    {"id": 4, "name": "test data4", "date": date(2023, 4, 4), "memo": None},
    {"id": 5, "name": "test data5", "date": date(2023, 5, 5), "memo": None},
]


class TestMysqlSourcePipeline:
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

    def test_postgres_to_bigquery_pipeline(
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
