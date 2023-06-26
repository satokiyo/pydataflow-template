import abc
import logging
from typing import List, Literal, Optional, Tuple

from configs.config_base import ModuleCommonParams
from configs.validator_base import Validator
from exceptions import ParameterValidationError
from ios.ios import IoType

logger = logging.getLogger(__name__)


class SinkConfigCommonParams(ModuleCommonParams):
    input: str = ""
    mode: Literal["replace", "append", "merge"] = "replace"
    merge_keys: Optional[List[str]] = None


class SinkConfigBase(SinkConfigCommonParams):
    """
    A base class for sink configurations.
    """

    pass


class IncrementalSinkConfigBase(SinkConfigCommonParams):
    """
    A base class for incremental sink configurations.

    Attributes:
        delete_query_ : str
            The query to be executed to delete records from the destination table based on a given range of incremental_column.
            This attribute is set when DELETE statement is required for incremental update with incremental_column as a merge key.
    """

    delete_query_: str = ""

    @abc.abstractmethod
    def get_incremental_interval_from_destination_table(
        self,
        incremental_column: str,
        destination_search_range: str,
        io_type: IoType,
    ) -> Tuple[str, str]:
        """
        Abstract method to get incremental interval from destination table.

        Parameters:
            incremental_column (str): The name of the column used as an incremental key.
            destination_search_range (str): The search range of the destination table.
            io_type: Type of IO to be used.

        Returns:
            Tuple containing the incremental interval and the data type of the incremental column.
        """
        raise NotImplementedError()


class SinkValidator(Validator):
    def validate(self, config: SinkConfigCommonParams, is_incremental=False):
        if is_incremental:
            return True

        errors = []

        if not config.name:
            errors.append("name is not specified.")

        if not config.module:
            errors.append("module is not specified.")

        if not config.input:
            errors.append("input is not specified.")

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True


class IncrementalSinkValidator(Validator):
    def validate(self, config: IncrementalSinkConfigBase, is_incremental=False) -> bool:
        if not is_incremental:
            return True

        if config.mode not in ["append", "merge"]:
            logger.warn(
                "The mode of the sink configuration is expected to be 'append' or 'merge' in incremental mode, but 'replace' is specified."
            )

        errors = []

        if config.mode == "merge" and not config.merge_keys:
            errors.append("merge_keys is not specified in incremental mode.")

        if config.mode != "merge" and config.merge_keys:
            errors.append("merge_keys is specified, but the mode is not 'merge'.")

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True
