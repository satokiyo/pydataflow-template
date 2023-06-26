import argparse
import logging.config
import os
from typing import Dict, List, Optional, Set

import apache_beam as beam
from configs.config_factory import (
    ConfigFactory,
    SinkConfigsUnion,
    SourceConfigsUnion,
    TransformConfigsUnion,
)
from configs.pipeline_config import PipelineConfig, PipelineConfigBuilder
from ios.ios import IoAdapter, IoType
from logger_settings import LoggingSettings
from modules.fcollection import FCollection
from modules.module_factory import ModuleFactory, ModuleName, ModuleProxy, ModuleType

logger = logging.getLogger(__name__)


def set_source_result(
    module_proxy: ModuleProxy,
    pipeline: beam.Pipeline,
    sources: List[SourceConfigsUnion],
    outputs: Dict[str, FCollection],
    executed_module_names: Set[str],
) -> None:
    for conf in sources:
        name = conf.name
        module_name = conf.module
        # skip already done module.
        if name in executed_module_names:
            continue
        output = module_proxy.expand(
            module_type=ModuleType("source"),
            module_name=ModuleName(module_name),
            pipeline=pipeline,
            input=FCollection(name="begin", pcol=pipeline, schema=None),
            config=conf,
        )
        outputs[name] = output
        executed_module_names.add(name)
        logger.info("expand source data. name:%s module:%s", name, module_name)


def set_transform_result(
    module_proxy: ModuleProxy,
    pipeline: beam.Pipeline,
    transforms: List[TransformConfigsUnion],
    outputs: Dict[str, FCollection],
    executed_module_names: Set[str],
) -> None:
    not_done_modules = []
    for conf in transforms:
        name = conf.name
        module_name = conf.module
        input_names = conf.inputs
        # skip already done module.
        if name in executed_module_names:
            continue

        # The keys (multiple) in "inputs" are not all present in "outputs" (not ready).
        if set(input_names).intersection(outputs.keys()) != set(input_names):
            not_done_modules.append(conf)
            continue

        inputs = [outputs.get(input) for input in input_names]

        # expand
        output = module_proxy.expand(
            module_type=ModuleType("transform"),
            module_name=ModuleName(module_name),
            pipeline=pipeline,
            inputs=inputs,
            config=conf,
        )
        outputs[name] = output
        executed_module_names.add(name)
        logger.info("expand transform data. name:%s module:%s", name, module_name)

    if not_done_modules:
        # recursive
        set_transform_result(
            module_proxy, pipeline, not_done_modules, outputs, executed_module_names
        )


def set_sink_result(
    module_proxy: ModuleProxy,
    pipeline: beam.Pipeline,
    sinks: List[SinkConfigsUnion],
    outputs: Dict[str, FCollection],
    executed_module_names: Set[str],
) -> None:
    not_done_modules = []
    for conf in sinks:
        name = conf.name
        module_name = conf.module
        input_name = conf.input
        # skip already done module.
        if name in executed_module_names:
            continue

        # The keys (multiple) in "inputs" are not all present in "outputs" (not ready).
        if input_name not in outputs.keys():
            not_done_modules.append(conf)
            continue

        input = outputs.get(input_name)

        # expand
        output = module_proxy.expand(
            ModuleType("sink"),
            module_name=ModuleName(module_name),
            pipeline=pipeline,
            input=input,
            config=conf,
        )
        outputs[name] = output
        executed_module_names.add(name)
        logger.info("expand sink data. name:%s module:%s", name, module_name)

    if not_done_modules:
        # recursive
        set_sink_result(module_proxy, pipeline, not_done_modules, outputs, executed_module_names)


def run(module_proxy: ModuleProxy, pipeline_config: PipelineConfig, options):
    """Run the beam pipeline"""

    with beam.Pipeline(options=options) as pipeline:
        num_modules = len(pipeline_config.get_module_names())
        executed_module_names: Set[str] = set()
        outputs: Dict[str, FCollection] = {}
        logger.info("create pipeline. num_modules: %d", num_modules)

        # expand graphs
        while len(executed_module_names) < num_modules:
            set_source_result(
                module_proxy,
                pipeline,
                pipeline_config.sources,
                outputs,
                executed_module_names,
            )
            set_transform_result(
                module_proxy, pipeline, pipeline_config.transforms, outputs, executed_module_names
            )
            set_sink_result(
                module_proxy, pipeline, pipeline_config.sinks, outputs, executed_module_names
            )


def main(
    pipeline_config_file_path: str,
    profiles_file_path: str,
    gcs_temp_folder: str,
    beam_args: Optional[List[str]] = None,
    log_level=None,
) -> None:
    """Compose objects and execute the beam pipeline"""

    # logging settings
    logging.config.dictConfig(LoggingSettings.get_setting_dict(log_level))

    # pipeline options
    options = beam.options.pipeline_options.PipelineOptions(
        beam_args, save_main_session=True, streaming=False
    )
    logger.info("pipeline options: %s", options._all_options)

    # configs
    logger.info(
        "get config from json.\nconfig_json:%s\nprofiles_json:%s",
        pipeline_config_file_path,
        profiles_file_path,
    )

    io_adapter = IoAdapter()

    if os.path.isfile(pipeline_config_file_path):
        io_type = IoType.LOCAL_FILE_IO
    else:
        io_type = IoType.GCS_FILE_IO
    pipeline_config_dict = io_adapter.read(io_type=io_type, file_path=pipeline_config_file_path)

    if not profiles_file_path:
        profiles_dict = {}
    else:
        if os.path.isfile(profiles_file_path):
            io_type = IoType.LOCAL_FILE_IO
        else:
            io_type = IoType.GCS_FILE_IO
        profiles_dict = io_adapter.read(io_type=io_type, file_path=profiles_file_path)

    # pipeline config
    pipeline_config = PipelineConfigBuilder(
        ConfigFactory(io_adapter=io_adapter, gcs_temp_folder=gcs_temp_folder)
    ).build(pipeline_config_dict, profiles_dict)

    # module proxy
    module_proxy = ModuleProxy(ModuleFactory())

    # run pipeline
    run(module_proxy, pipeline_config, options)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        required=True,
        type=str,
        help="Specify the path str to config json file.",
    )
    parser.add_argument(
        "--profile",
        default="",
        type=str,
        help="Specify the path str to connection profile json file.",
    )
    parser.add_argument(
        "--gcs_temp_folder",
        type=str,
        help="Specify the path str to gcs temp bucket.",
    )
    parser.add_argument(
        "--direct_runner",
        action="store_true",
        help="Use direct runner.",
    )

    args, beam_args = parser.parse_known_args()

    log_level = logging.INFO
    if args.direct_runner:
        beam_args.append("--runner=DirectRunner")
        log_level = logging.DEBUG

    beam_args.append("--experiments=use_runner_v2")
    beam_args.append("--sdk_location=container")
    beam_args.append(
        "--sdk_container_image=asia-northeast2-docker.pkg.dev/py-dataflow/pydataflow-repo/pydataflow-worker:latest"
    )

    main(
        pipeline_config_file_path=args.config,
        profiles_file_path=args.profile,
        gcs_temp_folder=args.gcs_temp_folder,
        beam_args=beam_args,
        log_level=log_level,
    )
