import apache_beam as beam
from modules.fcollection import FCollection


class Module:
    @classmethod
    def get_name(cls) -> str:
        raise NotImplementedError()

    def expand(self, pipeline: beam.Pipeline, input: FCollection, config) -> FCollection:
        raise NotImplementedError()


class SinkModule(Module, beam.PTransform):
    def get_type(self) -> str:
        return "source"


class SourceModule(Module, beam.PTransform):
    def get_type(self) -> str:
        return "transform"


class TransformModule(Module, beam.PTransform):
    def get_type(self) -> str:
        return "sink"
