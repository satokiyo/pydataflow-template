from logging import getLogger

import apache_beam as beam
from beam_postgres import splitters
from beam_postgres.io import ReadFromPostgres
from configs.source.postgres_source_config import PostgresSourceConfig
from ios.ios import IoType
from modules.fcollection import FCollection
from modules.module import SourceModule

logger = getLogger(__name__)


class PostgresSource(SourceModule):
    @classmethod
    def get_name(cls) -> str:
        return "postgres"

    def expand(
        self,
        pipeline: beam.Pipeline,
        input: FCollection,
        config: PostgresSourceConfig,
    ) -> FCollection:
        # for debug
        # logger.debug(f"Module: {PostgresSource.get_name()}")
        # logger.debug(f"parameters: {config.to_json()}")

        if config._QUERY_SEPARATOR in config.query.sql:
            splitter = splitters.QuerySplitter(sep=config._QUERY_SEPARATOR)
        else:
            splitter = splitters.NoSplitter()

        if config._QUERY_SEPARATOR in config.query.sql:
            stmt = config.query.sql.split(config._QUERY_SEPARATOR)[0]
        else:
            stmt = config.query.sql

        metadata = config.io_adapter.get_metadata(
            io_type=IoType(self.get_name()),
            config={
                "host": config.host,
                "database": config.database,
                "user": config.user,
                "password": config.password,
                "port": config.port,
            },
            query=stmt,
        )
        if metadata.row_count == 0:
            logger.warn(f"input is empty. skip. name: {config.name}")
            return FCollection(name=config.name, pcol=None)

        # apply transform
        output = pipeline | f"ReadFrom{config.module.upper()}_{config.name}" >> ReadFromPostgres(
            host=config.host,
            database=config.database,
            user=config.user,
            password=config.password,
            port=config.port,
            splitter=splitter,
            query=config.query.sql,
        )

        return FCollection(name=config.name, pcol=output, schema=metadata.schema)
