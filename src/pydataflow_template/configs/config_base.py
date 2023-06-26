import copy
import json
import logging
from typing import Dict

from configs.validator_base import ValidationPolicy
from ios.ios import IoAdapter

logger = logging.getLogger(__name__)


class ModuleCommonParams:
    name: str = ""
    module: str = ""


class ModuleConfigBase:
    """
    A base configuration class for modules.

    Attributes:
        policy (ValidationPolicy): The validation policy to use.
        io_adapter (IoAdapter): The I/O adapter to use.
        __original_param_dict (dict): The original parameter dictionary.
    """

    def __init__(
        self,
        policy: ValidationPolicy,
        io_adapter: IoAdapter,
    ):
        """
        Initializes a new instance of the ModuleConfigBase class.

        Args:
            policy (ValidationPolicy): The validation policy to use.
            io_adapter (IoAdapter): The I/O adapter to use.
        """
        self.policy = policy
        self.io_adapter = io_adapter
        self.__original_param_dict = {}

    def from_dict(self, **param_dict: Dict) -> "ModuleConfigBase":
        """
        Updates the instance attributes with parameters from a dictionary.

        Args:
            **param_dict: The dictionary containing the parameters.

        Returns:
            The updated instance.
        """
        # Store the original parameter dictionary.
        self.__original_param_dict = param_dict

        # Extract common parameters (excluding "parameters" key).
        common_params = {k: v for k, v in param_dict.items() if k != "parameters"}

        # Extract module unique parameters (excluding "profile" key).
        module_params = param_dict["parameters"].copy()
        module_params.pop("profile", None)

        # Merge common and module unique parameters into a single dictionary.
        all_params = {**common_params, **module_params}

        # Update instance dictionary with the merged parameters.
        self.__dict__.update(all_params)

        # Return the updated instance.
        return self

    def validate(self, **kargs) -> bool:
        """
        Validates the configuration instance.

        Args:
            **kargs: Additional arguments to be passed to the policy validation function.

        Returns:
            True if the instance is valid, False otherwise.
        """
        return self.policy.validate_all(self, **kargs)

    def get_original_param_dict(self) -> Dict:
        """
        Gets a deep copy of the original parameter dictionary.

        Returns:
            A deep copy of the original parameter dictionary.
        """
        return copy.deepcopy(self.__original_param_dict)

    def update_module_params(self, **param_dict: Dict) -> "ModuleConfigBase":
        """
        Updates the module parameters with the given dictionary.

        Args:
            **param_dict: The dictionary containing the new module parameters.

        Returns:
            The updated instance.
        """
        return self.from_dict(**param_dict)

    def to_json(self) -> str:
        """
        Converts the instance to a JSON string.

        Returns:
            The JSON string representing the instance.
        """
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
