from typing import Optional

import apache_beam as beam
from ios.schema import Schema


class FCollection:
    def __init__(
        self,
        name: str,
        pcol: beam.PCollection,
        schema: Optional[Schema] = None,
        schema_pcol: Optional[beam.PCollection] = None,
    ):
        self.name = name
        self.pcol = pcol
        self.schema = schema
        self.schema_pcol = schema_pcol
