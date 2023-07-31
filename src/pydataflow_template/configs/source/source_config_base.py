import abc
from datetime import datetime, timedelta
import logging
import re
from typing import Tuple

from configs.config_base import ModuleCommonParams
from configs.validator_base import Validator
from exceptions import ParameterValidationError
import pytz

logger = logging.getLogger(__name__)


class SourceConfigCommonParams(ModuleCommonParams):
    incremental: bool = False


class IncrementalSourceParams:
    incremental_column: str = ""
    incremental_interval_from: str = "max_value_in_destination"
    destination_sink_name: str = ""


class SourceConfigBase(SourceConfigCommonParams):
    """
    A base class for source configurations.
    """

    pass


class IncrementalSourceConfigBase(SourceConfigCommonParams, IncrementalSourceParams):
    """
    A base class for incremental source configurations.

    Attributes:
        INTERVAL_REGEXP (re.Pattern): A regular expression pattern used to extract the value and unit of the incremental interval from a string.
    """

    INTERVAL_REGEXP = re.compile(r"(^[1-9][0-9]*)([a-z].*)")

    def get_incremental_interval_from_params(self, timezone: pytz.timezone) -> Tuple[str, str]:
        """
        Gets the incremental interval from the configuration parameters and returns it as a tuple of the incremental interval
        start time and the data type of the column used for incremental updates.
        Return the time representation in the specified time zone.

        Returns:
            Tuple[str, str]: A tuple containing the incremental interval start time and the data type of the column used for
            incremental updates.
        Raises:
            ValueError: If the incremental interval is invalid or has an invalid unit.
        """
        x, unit = self.INTERVAL_REGEXP.match(self.incremental_interval_from.lower()).group(1, 2)
        x = int(x)
        if unit == "min":
            incremental_interval_from = (datetime.now(timezone) - timedelta(minutes=x)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        elif unit == "hour":
            incremental_interval_from = (datetime.now(timezone) - timedelta(hours=x)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        else:
            incremental_interval_from = (datetime.now(timezone) - timedelta(days=x)).strftime(
                "%Y-%m-%d 23:59:59"
            )

        column_data_type = "TIMESTAMP"
        return incremental_interval_from, column_data_type

    @abc.abstractclassmethod
    def get_incremental_query(
        self, incremental_interval_from: str, cast_type: str, sql_query: str
    ) -> str:
        """
        Adds an incremental condition to the given SQL query based on the provided incremental interval start time and data type
        of the column used for incremental updates.

        Args:
            incremental_interval_from (str): The incremental interval start time.
            cast_type (str): The data type of the column used for incremental updates.
            sql_query (str): The SQL query to which the incremental condition needs to be added.

        Returns:
            str: The modified SQL query with the incremental condition added.
        """
        raise NotImplementedError()


class SourceValidator(Validator):
    def validate(self, config: SourceConfigCommonParams, is_incremental: bool = False) -> bool:
        if is_incremental:
            return True

        errors = []

        if not config.name:
            errors.append("Please specify the name in the source configuration.")

        if not config.module:
            errors.append("Please specify the module in the source configuration.")

        if not isinstance(config.incremental, bool):
            errors.append(
                "The specified incremental mode is invalid. Please set it to either True or False."
            )

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True


class IncrementalSourceValidator(Validator):
    UNIT_OPTIONS = ["min", "hour", "day"]

    def validate(self, config: IncrementalSourceParams, is_incremental: bool = False) -> bool:
        if not is_incremental:
            return True

        errors = []

        if not config.incremental_column:
            errors.append(
                "Please specify the 'incremental_column' in the source configuration for incremental mode."
            )

        if not config.incremental_interval_from:
            errors.append(
                "Please specify the 'incremental_interval_from' in the source configuration for incremental mode."
            )
        elif config.incremental_interval_from == "max_value_in_destination":
            if not config.destination_sink_name:
                errors.append(
                    "Please specify the 'destination_sink_name' in the source configuration when 'incremental_interval_from' is set to 'max_value_in_destination'."
                )
        else:
            m = IncrementalSourceConfigBase.INTERVAL_REGEXP.match(
                config.incremental_interval_from.lower()
            )

            if not m:
                errors.append("The format of 'incremental_interval_from' is invalid.")
            else:
                x, unit = m.group(1, 2)

                try:
                    x = int(x)
                except ValueError:
                    errors.append(
                        "The format of 'incremental_interval_from' is invalid. 'X' must be an integer string."
                    )

                if unit not in self.UNIT_OPTIONS:
                    errors.append(
                        "The format of 'incremental_interval_from' is invalid. 'unit' must be in [min, hour, day]."
                    )

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True
