from logging import getLogger

import apache_beam as beam
from configs.sink.bigquery_sink_config import BigQuerySinkConfig
from ios.converter import PYTHON_BIGQUERY_TYPE_MAP
from ios.ios import IoType
from modules.fcollection import FCollection
from modules.module import SinkModule
from modules.sink.merger.bigquery_delete import BatchKeys, DeleteFromBigQuery

logger = getLogger(__name__)


class BigQuerySink(SinkModule):
    WRITE_DISPOSITION_MAP = {
        "replace": "WRITE_TRUNCATE",
        "append": "WRITE_APPEND",
        "merge": "WRITE_APPEND",
    }

    @classmethod
    def get_name(cls) -> str:
        return "bigquery"

    def expand(
        self,
        pipeline: beam.Pipeline,
        input: FCollection,
        config: BigQuerySinkConfig,
    ) -> FCollection:
        # for debug
        # logger.debug(f"Module: {BigQuerySink.get_name()}")
        # logger.debug(f"parameters: {config.to_json()}")

        if input.pcol is None:
            logger.warn(f"input is empty. skip. name: {config.name}")
            return FCollection(name=config.name, pcol=None)

        additional_bq_parameters = config.get_additional_bq_parameters()
        pcol: beam.PCollection = input.pcol

        if config.schema == "get_from_table":
            # Retrieve schema from the BigQuery table
            table_ref = config.table
            _, table = table_ref.split(":")
            stmt = f"select * from {table}"
            schema = config.io_adapter.get_metadata(IoType.BIGQUERY_RDB_IO, query=stmt).schema
            schema_pcol = (
                pipeline
                | f"CreateSchemaPcollection {config.module.upper()}_{config.name}"
                >> beam.Create([schema.to_json(target_system="bigquery")])
                | beam.Map(lambda x: {"fields": x})
            )
        else:
            schema = input.schema
            schema_pcol = input.schema_pcol

        # In the case of mode=merge, duplicates are eliminated by deleting the differential records from the target table (delete+insert).
        # In the case of mode=append, since source.incremental_interval_from=max_value_in_destination, there is no need for deletion.
        if config.mode == "merge":
            if not schema:
                msg = "In the case of mode=merge, it is not possible to dynamically retrieve the schema."
                logger.error(msg)
                return FCollection(name=config.name, pcol=None)
            if len(config.merge_keys) == 1 and (config.merge_keys[0] == config.partitioning_field):
                # Incremental update with incremental_column as a merge key: delete+insert
                # delete_query should be prepared in advance using the incremental_interval_from specified in the source.
                logger.info("Incremental update with incremental_column as a merge key.")
                delete_query = config.delete_query_
                _ = config.io_adapter.read(io_type=IoType.BIGQUERY_RDB_IO, query=delete_query)
                logger.info("Done delete query: %s", delete_query)
            else:
                # Incremental update with merge_keys as merge keys: delete+insert or merge
                # impl1. delete, then insert
                logger.info("Incremental update with merge_keys.")
                _ = (
                    pcol
                    | f"Batch keys {config.module.upper()}_{config.name}"
                    >> beam.ParDo(BatchKeys(merge_keys=config.merge_keys, batch_size=10000))
                    | f"Delete from BigQuery {config.module.upper()}_{config.name}"
                    >> beam.ParDo(
                        DeleteFromBigQuery(
                            config.table,
                            schema=schema,
                            merge_keys=config.merge_keys,
                            num_threads=10,
                            batch_size=1000,
                        )
                    )
                )
                # TODO
                # impl2. Read from destination, merge, then truncate

            # apply transform
            output = (
                pcol
                | f"WriteTo{config.module.upper()}_{config.name}"
                >> beam.io.WriteToBigQuery(
                    table=config.table,
                    schema={"fields": schema.to_json(target_system="bigquery")},
                    custom_gcs_temp_location=config.gcs_temp_location,
                    write_disposition=self.WRITE_DISPOSITION_MAP[config.mode.lower()],
                    create_disposition=config.create_disposition,
                    additional_bq_parameters=additional_bq_parameters,
                )
            )

            return FCollection(name=config.name, pcol=output, schema_pcol=schema_pcol)

        if not schema_pcol:

            def filter_out_nones(elem):
                return None not in list(elem.values())

            def get_bq_schema_from_element(elem):
                fields = [
                    {"name": k, "type": PYTHON_BIGQUERY_TYPE_MAP[type(v)], "mode": "NULLABLE"}
                    for k, v in elem.items()
                ]
                return {"fields": fields}

            element = (
                pcol
                | f"FilterOutNones {config.module.upper()}_{config.name}"
                >> beam.Filter(filter_out_nones)
                | f"SampleOneRow {config.module.upper()}_{config.name}"
                >> beam.combiners.Sample.FixedSizeGlobally(1)
            )
            schema_pcol = (
                element
                | f"GetSchemaFromPcollection {config.module.upper()}_{config.name}"
                >> beam.Map(lambda x: get_bq_schema_from_element(x[0]))
            )

        DESTINATION_TABLE = config.table
        table_dest_schema_map = (
            schema_pcol
            | f"MakeSchemas {config.module.upper()}_{config.name}"
            >> beam.Map(lambda x: (DESTINATION_TABLE, x))
        )

        table_name_dest_map = (
            pipeline
            | f"CreateTableNameDestinationMap {config.module.upper()}_{config.name}"
            >> beam.Create([("table_name", DESTINATION_TABLE)])
        )

        # apply transform
        output = pcol | f"WriteTo{config.module.upper()}_{config.name}" >> beam.io.WriteToBigQuery(
            table=lambda _x, table_name_dest_map: (table_name_dest_map["table_name"]),
            table_side_inputs=(beam.pvalue.AsDict(table_name_dest_map),),
            schema=lambda dest, table_dest_schema_map: table_dest_schema_map.get(dest, None),
            schema_side_inputs=(beam.pvalue.AsDict(table_dest_schema_map),),
            method="DEFAULT",
            custom_gcs_temp_location=config.gcs_temp_location,
            write_disposition=self.WRITE_DISPOSITION_MAP[config.mode.lower()],
            create_disposition=config.create_disposition,
            additional_bq_parameters=additional_bq_parameters,
        )

        return FCollection(name=config.name, pcol=output, schema_pcol=schema_pcol)
