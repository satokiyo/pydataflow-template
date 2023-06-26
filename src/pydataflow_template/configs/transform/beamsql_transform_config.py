import logging
from typing import List, Optional

from configs.config_base import ModuleConfigBase
from configs.transform.transform_config_base import TransformConfigCommonParams, TransformValidator
from configs.validator_base import ValidationPolicy, Validator
from exceptions import ParameterValidationError

logger = logging.getLogger(__name__)


class BeamSqlTransformConfigParams(TransformConfigCommonParams):
    sql: str = ""
    inputs_alias: Optional[List[str]] = None


class BeamSqlTransformConfig(ModuleConfigBase, BeamSqlTransformConfigParams):
    def from_dict(self, **param_dict):
        return super().from_dict(**param_dict)


class BeamSqlTransformValidator(Validator):
    def validate(self, config: BeamSqlTransformConfig, is_incremental=False) -> bool:
        if is_incremental:
            return True

        errors = []

        if not config.sql:
            errors.append("Please specify the sql in the transform configuration.")

        if config.inputs_alias:
            if not isinstance(config.inputs_alias, list):
                errors.append(
                    "Please specify the inputs_alias in the transform configuration as list."
                )
            if len(config.inputs_alias) != len(config.inputs):
                errors.append(
                    "Please ensure that the inputs_alias and the inputs are specified with the same length in the transform configuration"
                )

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True


class BeamSqlTransformValidationPolicy(ValidationPolicy):
    def __init__(self):
        super().__init__()
        self.add(TransformValidator())
        self.add(BeamSqlTransformValidator())
