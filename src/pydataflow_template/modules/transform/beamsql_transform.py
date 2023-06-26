from logging import getLogger
from typing import List, NamedTuple

import apache_beam as beam
from apache_beam import coders
from apache_beam.transforms.sql import SqlTransform
from configs.transform.beamsql_transform_config import BeamSqlTransformConfig
from modules.fcollection import FCollection
from modules.module import TransformModule

logger = getLogger(__name__)


class BeamSqlTransform(TransformModule):
    @classmethod
    def get_name(cls) -> str:
        return "postgres"

    def expand(
        self,
        pipeline: beam.Pipeline,
        inputs: List[FCollection],
        config: BeamSqlTransformConfig,
    ) -> FCollection:
        is_empty_inputs = any([fcol.pcol is None for fcol in inputs])
        if is_empty_inputs:
            logger.warn(f"input is empty. skip. name: {config.name}")
            return FCollection(name=config.name, pcol=None)

        pcols_with_schema = {}
        for i, fcol in enumerate(inputs):
            input = fcol.pcol
            schema = fcol.schema

            input_schema = [
                (field_dict["name"], field_dict["type"]) for field_dict in schema.to_json("python")
            ]
            named_tuple_schema = NamedTuple(f"SCHEMA{i}", input_schema)
            coders.registry.register_coder(named_tuple_schema, coders.RowCoder)

            # apply transform
            pcol_with_schema = (
                input
                | f"To Row with Schema {config.module.upper()}_{fcol.name}_{i}"
                >> beam.Map(lambda x: beam.Row(**x)).with_output_types(named_tuple_schema)
            )
            pcols_with_schema[fcol.name] = pcol_with_schema

        pcols_with_alias = (
            {
                alias: pcols_with_schema[name]
                for alias, name in zip(config.inputs_alias, config.inputs)
            }
            if config.inputs_alias
            else pcols_with_schema
        )

        output = (
            pcols_with_alias
            | f"SqlTransform {config.module.upper()}_{config.name}" >> SqlTransform(config.sql)
            | f"To Dict {config.module.upper()}_{config.name}" >> beam.Map(lambda x: x._asdict())
        )

        return FCollection(name=config.name, pcol=output)
