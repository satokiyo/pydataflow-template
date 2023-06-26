# Standard Library
from logging import getLogger

# Third Party Library
import apache_beam as beam
from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL
from configs.source.mysql_source_config import MySqlSourceConfig
from ios.ios import IoType
from modules.fcollection import FCollection
from modules.module import SourceModule

logger = getLogger(__name__)


class MySqlSource(SourceModule):
    @classmethod
    def get_name(cls) -> str:
        return "mysql"

    def expand(
        self,
        pipeline: beam.Pipeline,
        input: FCollection,
        config: MySqlSourceConfig,
    ) -> FCollection:
        # for debug
        # logger.debug(f"Module: {MySqlSource.get_name()}")
        # logger.debug(f"parameters: {config.to_json()}")

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
        output = pipeline | f"ReadFrom{config.module.upper()}_{config.name}" >> ReadFromMySQL(
            query=config.query.sql,
            host=config.host,
            database=config.database,
            user=config.user,
            password=config.password,
            port=config.port,
            splitter=splitters.NoSplitter(),
        )

        return FCollection(name=config.name, pcol=output, schema=metadata.schema)
