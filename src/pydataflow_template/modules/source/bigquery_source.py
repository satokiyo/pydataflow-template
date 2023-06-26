from logging import getLogger

import apache_beam as beam
from configs.source.bigquery_source_config import BigQuerySourceConfig
from ios.ios import IoType
from modules.fcollection import FCollection
from modules.module import SourceModule

logger = getLogger(__name__)


class BigQuerySource(SourceModule):
    @classmethod
    def get_name(cls) -> str:
        return "bigquery"

    def expand(
        self,
        pipeline: beam.Pipeline,
        input: FCollection,
        config: BigQuerySourceConfig,
    ) -> FCollection:
        # for debug
        # logger.debug(f"Module: {BigQuerySource.get_name()}")
        # logger.debug(f"parameters: {config.to_json()}")

        stmt = config.query.sql
        metadata = config.io_adapter.get_metadata(
            io_type=IoType(self.get_name()),
            query=stmt,
        )
        if metadata.row_count == 0:
            logger.warn(f"input is empty. skip. name: {config.name}")
            return FCollection(name=config.name, pcol=None)

        # apply transform
        output = (
            pipeline
            | f"ReadFrom{config.module.upper()}_{config.name}"
            >> beam.io.ReadFromBigQuery(
                gcs_location=config.gcs_temp_location,
                query=config.query.sql,
                project=config.project,
                use_standard_sql=True,
            )
        )

        return FCollection(name=config.name, pcol=output, schema=metadata.schema)
