import json

import pytest

from pydataflow_template.configs.config_factory import ConfigFactory
from pydataflow_template.configs.pipeline_config import PipelineConfigBuilder
from pydataflow_template.ios.ios import IoAdapter, IoType
from tests.helper.utils import update_config_json

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
    "transforms": [
        {
            "name": "transform",
            "module": "beamsql",
            "inputs": ["in"],
            "parameters": {
                "sql": "SELECT id, count(*) AS `count` FROM a GROUP BY id ORDER BY count DESC",
                "inputs_alias": ["a"],
            },
        }
    ],
    "sinks": [
        {
            "name": "out",
            "module": "bigquery",
            "input": "transform",
            "mode": "replace",
            "parameters": {
                "table": "my-prj:test.test",
            },
        }
    ],
}

PROFILE = {}


class TestBeamTransformConfig:
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
        return PipelineConfigBuilder(ConfigFactory(IoAdapter(), gcs_temp_folder="gs://test/"))

    def test_default_config_json(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        pipeline_config_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_config_json)
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        _ = config_builder.build(pipeline_config_dict, profiles_dict)

    @pytest.mark.parametrize(
        "update_data",
        [
            {"transforms": [{"name": ""}]},
            {"transforms": [{"name": None}]},
            {"transforms": [{"module": ""}]},
            {"transforms": [{"module": None}]},
            {"transforms": [{"inputs": "not list"}]},
            {"transforms": [{"inputs": None}]},
            {"transforms": [{"parameters": {"sql": ""}}]},
            {"transforms": [{"parameters": {"sql": None}}]},
            {"transforms": [{"parameters": {"inputs_alias": "not list"}}]},
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

    def test_inputs_and_inputs_alias_must_be_same_length(
        self, base_config_json, base_profiles_json, config_builder, io_adapter
    ):
        update_data = {
            "transforms": [
                {
                    "inputs": ["in1", "in2"],
                    "parameters": {
                        "inputs_alias": ["a", "b", "c"],
                    },
                }
            ],
        }

        pipeline_config_file_path = update_config_json(update_data, base_config_json)
        pipeline_config_dict = io_adapter.read(
            IoType.LOCAL_FILE_IO, file_path=pipeline_config_file_path
        )
        profiles_dict = io_adapter.read(IoType.LOCAL_FILE_IO, file_path=base_profiles_json)

        with pytest.raises(Exception):
            config_builder.build(pipeline_config_dict, profiles_dict)
