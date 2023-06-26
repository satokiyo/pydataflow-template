import os
from typing import Union

from configs.sink.bigquery_sink_config import BigQuerySinkConfig, BigQuerySinkValidationPolicy
from configs.source.bigquery_source_config import (
    BigQuerySourceConfig,
    BigQuerySourceValidationPolicy,
)
from configs.source.mysql_source_config import MySqlSourceConfig, MySqlSourceValidationPolicy
from configs.source.postgres_source_config import (
    PostgresSourceConfig,
    PostgresSourceValidationPolicy,
)
from configs.transform.beamsql_transform_config import (
    BeamSqlTransformConfig,
    BeamSqlTransformValidationPolicy,
)
from exceptions import ConfigNotRegisteredError
from ios.ios import IoAdapter
from modules.module_factory import ModuleName, ModuleType

SourceConfigsUnion = Union[
    BigQuerySourceConfig,
    MySqlSourceConfig,
    PostgresSourceConfig,
]
TransformConfigsUnion = BeamSqlTransformConfig
SinkConfigsUnion = BigQuerySinkConfig
ConfigsUnion = Union[SourceConfigsUnion, TransformConfigsUnion, SinkConfigsUnion]


class Singleton:
    """
    Singleton class that ensures only one instance of ConfigFactory is created.
    """

    def __new__(cls, *args, **kargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance


class ConfigFactory(Singleton):
    """
    Factory class for creating configuration objects.
    """

    def __init__(self, io_adapter: IoAdapter, gcs_temp_folder: str):
        self.io_adapter = io_adapter
        self.gcs_temp_folder = gcs_temp_folder
        self.__registry = {
            (ModuleType.SOURCE, ModuleName.MYSQL): self._create_mysql_source_config,
            (ModuleType.SOURCE, ModuleName.BIGQUERY): self._create_bigquery_source_config,
            (ModuleType.SOURCE, ModuleName.POSTGRES): self._create_postgres_source_config,
            (ModuleType.TRANSFORM, ModuleName.BEAMSQL): self._create_beamsql_transform_config,
            (ModuleType.SINK, ModuleName.BIGQUERY): self._create_bigquery_sink_config,
        }

    def get_module_config(self, module_type: ModuleType, module_name: ModuleName, **kargs):
        """
        Returns a config object for the given module type and name.
        """
        create_fn = self.__registry.get((module_type, module_name))
        if not create_fn:
            raise ConfigNotRegisteredError(
                f"Config of ModuleType.{module_type} and ModuleName.{module_name} is not registered."
            )
        return create_fn(**kargs)

    # source configs
    def _create_mysql_source_config(self, **kargs):
        return self._create_config(MySqlSourceConfig, MySqlSourceValidationPolicy, **kargs)

    def _create_bigquery_source_config(self, **kargs):
        return self._create_config(
            BigQuerySourceConfig,
            BigQuerySourceValidationPolicy,
            **kargs,
            gcs_temp_location=os.path.join(self.gcs_temp_folder, "read-from-bigquery"),
        )

    def _create_postgres_source_config(self, **kargs):
        return self._create_config(PostgresSourceConfig, PostgresSourceValidationPolicy, **kargs)

    # transform configs
    def _create_beamsql_transform_config(self, **kargs):
        return self._create_config(
            BeamSqlTransformConfig, BeamSqlTransformValidationPolicy, **kargs
        )

    # sink configs
    def _create_bigquery_sink_config(self, **kargs):
        return self._create_config(
            BigQuerySinkConfig,
            BigQuerySinkValidationPolicy,
            **kargs,
            gcs_temp_location=os.path.join(self.gcs_temp_folder, "write-to-bigquery"),
        )

    def _create_config(self, config_class, validation_policy_class, **kargs):
        return config_class(policy=validation_policy_class(), io_adapter=self.io_adapter).from_dict(
            **kargs
        )
