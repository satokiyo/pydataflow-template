import logging
from typing import List, Optional

from configs.config_base import ModuleCommonParams
from configs.validator_base import Validator
from exceptions import ParameterValidationError

logger = logging.getLogger(__name__)


class TransformConfigCommonParams(ModuleCommonParams):
    inputs: Optional[List[str]] = None


class TransformConfigBase(TransformConfigCommonParams):
    """
    A base class for transform configurations.
    """

    pass


class TransformValidator(Validator):
    def validate(self, config: TransformConfigCommonParams, is_incremental=False):
        if is_incremental:
            return True

        errors = []

        if not config.name:
            errors.append("Please specify the name in the transform configuration.")

        if not config.module:
            errors.append("Please specify the module in the transform configuration.")

        if not config.inputs or not isinstance(config.inputs, list):
            errors.append("Please specify the inputs in the transform configuration.")

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True
