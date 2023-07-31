import logging
from typing import List

from configs.config_factory import (
    ConfigFactory,
    SinkConfigsUnion,
    SourceConfigsUnion,
    TransformConfigsUnion,
)
from exceptions import ParameterValidationError
from ios.ios import IoType
from modules.module_factory import ModuleName, ModuleType
import pytz

logger = logging.getLogger(__name__)


class PipelineConfig:
    def __init__(
        self,
        sources: List[SourceConfigsUnion],
        transforms: List[TransformConfigsUnion],
        sinks: List[SinkConfigsUnion],
        timezone: pytz.timezone = pytz.timezone("Asia/Tokyo"),
    ) -> None:
        """Initializes the PipelineConfig class.

        Args:
            sources (List[SourceConfigsUnion]): A list of source configurations.
            transforms (List[TransformConfigsUnion]): A list of transform configurations.
            sinks (List[SinkConfigsUnion]): A list of sink configurations.
        """
        if not (sources and sinks):
            msg = "Either source config or sink config is not defined."
            logger.error(msg)
            raise ParameterValidationError(msg)

        names = [
            config.name for config_list in [sources, transforms, sinks] for config in config_list
        ]

        if len(names) > len(set(names)):
            msg = "Module name duplicated."
            logger.error(msg)
            raise ParameterValidationError(msg)

        self.sources = sources
        self.transforms = transforms
        self.sinks = sinks
        self.timezone = timezone

    def get_module_names(self) -> List[str]:
        """Returns a list of module names."""
        return [config.name for config in [*self.sources, *self.transforms, *self.sinks]]

    def validate(self) -> None:
        """Validates the parameter values for the module configs in the pipeline."""
        if not all(
            config.validate(is_incremental=False)
            for config in [*self.sources, *self.transforms, *self.sinks]
        ):
            raise ParameterValidationError("Validation failed")

        if any(source.incremental is True for source in self.sources):
            # incremental mode
            if len(self.sources) > 1:
                msg = "Multiple inputs are not supported in incremental mode."
                logger.error(msg)
                raise ParameterValidationError(msg)

            for source in self.sources:
                if source.incremental_interval_from != "max_value_in_destination":
                    sink = self._get_sink_ref(source)
                    if sink.mode == "append":
                        msg = "mode == append in the sink config is allowed only when incremental_interval_from == max_value_in_destination in the source config. Otherwise, mode == merge must be specified to avoid duplication."
                        logger.error(msg)
                        raise ParameterValidationError(msg)

            # validate params for incremental mode
            for config in [*self.sources, *self.transforms, *self.sinks]:
                assert config.validate(is_incremental=True) is True

        else:
            # full replace mode
            if any(sink.mode != "replace" for sink in self.sinks):
                msg = "The mode of the sink config must be 'replace' when the mode of the source config is not 'incremental'"
                logger.error(msg)
                raise ParameterValidationError(msg)

    def setup_incremental_mode(self) -> None:
        """Prepares the pipeline for incremental mode."""
        if any(source.incremental for source in self.sources):
            self._update_source_query_for_incremental_mode()

    def _get_source_ref(self, sink: SinkConfigsUnion) -> SourceConfigsUnion:
        """Returns the reference to the source configuration based on the sink input name."""
        return next(filter(lambda source: source.name == sink.input, self.sources), None)

    def _get_sink_ref(self, source: SourceConfigsUnion) -> SinkConfigsUnion:
        """Returns the reference to the sink configuration based on the source name."""
        return next(filter(lambda sink: sink.input == source.name, self.sinks), None)

    def _update_source_query_for_incremental_mode(self) -> None:
        """Updates the source query for incremental mode."""
        for i, source in enumerate(self.sources):
            if not source.incremental:
                continue

            sink = self._get_sink_ref(source)
            if source.incremental_interval_from != "max_value_in_destination":
                # If the interval is specified in the configuration.
                incremental_interval_from, _ = source.get_incremental_interval_from_params(
                    timezone=self.timezone
                )
                # In the case of range replacement (delete+insert), prepare a Delete statement.
                if (
                    sink.mode == "merge"
                    and len(sink.merge_keys) == 1
                    and (sink.merge_keys[0] == sink.partitioning_field)
                ):
                    # FIXME prepare delete queries not for each database separately in child classes.
                    delete_query = f"DELETE FROM {sink.table.split(':')[-1]} WHERE CAST({source.incremental_column} AS TIMESTAMP) > PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '{incremental_interval_from}', 'UTC')"
                    logger.info("save delete query %s", delete_query)
                    # update original params
                    original_params = sink.get_original_param_dict()
                    original_params.get("parameters").update({"delete_query_": delete_query})
                    updated_params = original_params
                    sink = sink.update_module_params(**updated_params)
            else:
                if source.destination_sink_name != sink.name:
                    msg = "The attribute destination_sink_name specified in source config not found in sink config."
                    logger.error(msg)
                    raise ParameterValidationError(msg)

                # When incremental_interval_from is set to "max_value_in_destination", the maximum value of the
                # incremental_column is retrieved from the destination table.
                incremental_interval_from = sink.get_incremental_interval_from_destination_table(
                    incremental_column=source.incremental_column,
                    io_type=IoType(sink.module),
                    timezone=self.timezone,
                )

            incremental_query = source.get_incremental_query(
                incremental_interval_from, timezone=self.timezone
            )
            logger.info(
                "overwrite source query (%s):\n(from) %s\n(to) %s",
                source.name,
                source.query.sql,
                incremental_query,
            )

            # update original params
            original_params = source.get_original_param_dict()
            original_params.get("parameters").update({"query": incremental_query})
            updated_params = original_params
            self.sources[i] = source.update_module_params(**updated_params)


class PipelineConfigBuilder:
    def __init__(self, config_factory: ConfigFactory):
        """
        Parameters
            config_factory(ConfigFactory) : A ConfigFactory object used to get config
        """
        self.config_factory = config_factory

    def build(self, pipeline_config_dict: dict, profiles_dict: dict) -> PipelineConfig:
        """
        Build a PipelineConfig object from pipeline_config_dict and profiles_dict

        Parameters
            pipeline_config_dict : dict
                A dictionary containing the pipeline configuration
            profiles_dict : dict
                A dictionary containing the profile information

        Returns
            PipelineConfig
                A PipelineConfig object built from pipeline_config_dict and profiles_dict
        """

        def build_configs(module_type, config_list):
            return [
                self.config_factory.get_module_config(
                    ModuleType(module_type),
                    ModuleName(s.get("module")),
                    **s,
                    **profiles_dict.get(s.get("parameters").get("profile"), {}),
                )
                for s in pipeline_config_dict.get(config_list, [])
            ]

        sources = build_configs("source", "sources")
        transforms = build_configs("transform", "transforms")
        sinks = build_configs("sink", "sinks")

        pipeline_config = PipelineConfig(sources, transforms, sinks)
        pipeline_config.validate()
        pipeline_config.setup_incremental_mode()

        return pipeline_config
