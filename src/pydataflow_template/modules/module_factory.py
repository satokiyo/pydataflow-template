from enum import Enum
import logging

from exceptions import ModuleNotRegisteredError
from modules.fcollection import FCollection
from modules.sink.bigquery_sink import BigQuerySink
from modules.source.bigquery_source import BigQuerySource
from modules.source.mysql_source import MySqlSource
from modules.source.postgres_source import PostgresSource
from modules.transform.beamsql_transform import BeamSqlTransform

logger = logging.getLogger(__name__)


class ModuleType(Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    SINK = "sink"

    def __eq__(self, other):
        if type(self).__qualname__ != type(other).__qualname__:
            return NotImplemented
        return self.name == other.name and self.value == other.value

    def __hash__(self):
        return hash((type(self).__qualname__, self.name))


class ModuleName(Enum):
    MYSQL = "mysql"
    BIGQUERY = "bigquery"
    POSTGRES = "postgres"
    BEAMSQL = "beamsql"

    def __eq__(self, other):
        if type(self).__qualname__ != type(other).__qualname__:
            return NotImplemented
        return self.name == other.name and self.value == other.value

    def __hash__(self):
        return hash((type(self).__qualname__, self.name))


class ModuleFactory:
    __all_modules = {
        (ModuleType.SOURCE, ModuleName.MYSQL): MySqlSource,
        (ModuleType.SOURCE, ModuleName.BIGQUERY): BigQuerySource,
        (ModuleType.SOURCE, ModuleName.POSTGRES): PostgresSource,
        (ModuleType.TRANSFORM, ModuleName.BEAMSQL): BeamSqlTransform,
        (ModuleType.SINK, ModuleName.BIGQUERY): BigQuerySink,
    }

    def __init__(self):
        self.__pool = {}

    def _get_module(self, module_type: ModuleType, module_name: ModuleName):
        cache = self.__pool.get((module_type, module_name), None)
        if cache:
            return cache

        obj = self.__all_modules.get((module_type, module_name), None)
        if not obj:
            raise ModuleNotRegisteredError(
                f"ModuleType.{module_type} and ModuleName.{module_name} is not registered."
            )
        obj_instance = obj()
        self.__pool[(module_type, module_name)] = obj_instance
        return obj_instance


class ModuleProxy:
    def __init__(self, factory: ModuleFactory):
        self.factory = factory

    def get_name(self, module_type: ModuleType, module_name: ModuleName) -> str:
        return self.factory._get_module(module_type, module_name).get_name()

    def expand(self, module_type: ModuleType, module_name: ModuleName, **kwargs) -> FCollection:
        return self.factory._get_module(module_type, module_name).expand(**kwargs)
