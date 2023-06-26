import logging
from typing import List, Protocol

logger = logging.getLogger(__name__)


class Validator(Protocol):
    def validate(self, config, is_incremental) -> bool:
        """
        Validates the configuration.

        Args:
            config: An instance of ModuleConfig class.
            is_incremental: A boolean indicating whether the configuration class is for the incremental mode.
        """
        raise NotImplementedError()


class ValidationPolicy:
    def __init__(self):
        self.validators: List[Validator] = []

    def add(self, validator: Validator) -> None:
        self.validators.append(validator)

    def validate_all(self, config, **kwargs) -> bool:
        """
        Validates the configuration using all registered validators.

        Args:
            config: An instance of ModuleConfig class.
            kwargs: Additional arguments to pass to the validators.

        Returns:
            A boolean indicating whether all validators passed or not.
        """
        for validator in self.validators:
            if not validator.validate(config, **kwargs):
                return False
        return True
