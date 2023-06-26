import copy
import logging

from configs.config_base import ModuleConfigBase
from configs.params_base import Query
from configs.source.source_config_base import (
    IncrementalSourceConfigBase,
    IncrementalSourceParams,
    IncrementalSourceValidator,
    SourceConfigCommonParams,
    SourceValidator,
)
from configs.validator_base import ValidationPolicy, Validator
from exceptions import ParameterValidationError

logger = logging.getLogger(__name__)


class BigQuerySourceConfigParams(SourceConfigCommonParams, IncrementalSourceParams):
    # bigquery source module parameters.
    query: Query = None
    project: str = ""
    gcs_temp_location: str = ""


class BigQuerySourceConfig(
    ModuleConfigBase, IncrementalSourceConfigBase, BigQuerySourceConfigParams
):
    def from_dict(self, **param_dict):
        copied = copy.deepcopy(param_dict)  # deep copy for update
        query = copied.get("parameters").get("query", None)

        if not query:
            msg = "required param query is not specified."
            logger.error(msg)
            raise ParameterValidationError(msg)

        if isinstance(query, Query):
            return super().from_dict(**param_dict)

        if query.endswith(".sql"):
            query = self.file_io.parse_file_to_str(query)
        copied.get("parameters").update({"query": Query(query)})
        updated = copied
        return super().update_module_params(**updated)

    def get_incremental_query(
        self, incremental_interval_from: str, cast_type: str, sql_query: str = ""
    ) -> str:
        sql_query = self.query.sql
        return super().get_incremental_query(
            incremental_interval_from, cast_type=cast_type, sql_query=sql_query
        )


class BigQuerySourceValidator(Validator):
    def validate(self, config: BigQuerySourceConfig, is_incremental=False) -> bool:
        if is_incremental:
            return True

        errors = []

        if not config.query:
            errors.append("Please specify the query in the source configuration.")

        if not config.project:
            errors.append("Please specify the project in the source configuration.")

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True


class BigQuerySourceValidationPolicy(ValidationPolicy):
    def __init__(self):
        super().__init__()
        self.add(SourceValidator())
        self.add(BigQuerySourceValidator())
        self.add(IncrementalSourceValidator())
