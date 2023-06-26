from enum import Enum
import logging
from typing import Any, Protocol, Union

from exceptions import IoError
from google.cloud import bigquery, storage
from ios.clients import MysqlClient, PostgresClient
from ios.providers import (
    BigQueryRdbProvider,
    GcpKvsProvider,
    GcsFileProvider,
    LocalFileProvider,
    MysqlRdbProvider,
    PostgresRdbProvider,
)
from ios.schema import MetaData, Schema
import pandas as pd

logger = logging.getLogger(__name__)


class IoType(Enum):
    LOCAL_FILE_IO = "local_file"
    GCS_FILE_IO = "gcs_file"
    BIGQUERY_RDB_IO = "bigquery"
    POSTGRES_RDB_IO = "postgres"
    MYSQL_RDB_IO = "mysql"
    GCP_KVS_IO = "gcp_kvs"

    def __eq__(self, other):
        if type(self).__qualname__ != type(other).__qualname__:
            return NotImplemented
        return self.name == other.name and self.value == other.value

    def __hash__(self):
        return hash((type(self).__qualname__, self.name))


class IoProtocol(Protocol):
    def read(self, **kargs) -> Union[Any, str, pd.DataFrame]:
        raise NotImplementedError()

    def get_metadata(self, **kargs) -> MetaData:
        raise NotImplementedError()

    def write(self, **kargs):
        raise NotImplementedError()


class Io:
    def __init__(self, provider: IoProtocol):
        self.provider = provider

    def read(self, **kargs) -> Union[Any, str, pd.DataFrame]:
        return self.provider.read(**kargs)

    def get_metadata(self, **kargs) -> MetaData:
        return self.provider.get_metadata(**kargs)

    def write(self, **kargs):
        return self.provider.write(**kargs)


class IoAdapter:
    def read(self, io_type: IoType, **kargs):
        io = Io(provider=self._get_provider(io_type))
        return io.read(**kargs)

    def get_metadata(self, io_type: IoType, **kargs):
        io = Io(provider=self._get_provider(io_type))
        return io.get_metadata(**kargs)

    def write(self, io_type: IoType, **kargs):
        io = Io(provider=self._get_provider(io_type))
        return io.write(**kargs)

    def _get_provider(self, io_type: IoType):
        if io_type == IoType.LOCAL_FILE_IO:
            return LocalFileProvider()
        elif io_type == IoType.GCS_FILE_IO:
            return GcsFileProvider(storage.Client())
        elif io_type == IoType.BIGQUERY_RDB_IO:
            return BigQueryRdbProvider(bigquery.Client(), schema=Schema())
        elif io_type == IoType.MYSQL_RDB_IO:
            return MysqlRdbProvider(MysqlClient(), schema=Schema())
        elif io_type == IoType.POSTGRES_RDB_IO:
            return PostgresRdbProvider(PostgresClient(), schema=Schema())
        elif io_type == IoType.GCP_KVS_IO:
            return GcpKvsProvider()
        else:
            msg = f"IoType not defined: {io_type}"
            logger.error(msg)
            raise IoError(msg)
