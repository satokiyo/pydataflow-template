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


class MySqlSourceConfigParams(SourceConfigCommonParams, IncrementalSourceParams):
    # mysql source module parameters and profile info.
    query: Query = None
    host: str = ""
    database: str = ""
    user: str = ""
    password: str = ""
    port: str = ""


class MySqlSourceConfig(ModuleConfigBase, IncrementalSourceConfigBase, MySqlSourceConfigParams):
    # mysql source module parameters and profile info.
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


class MysqlSourceValidator(Validator):
    def validate(self, config: MySqlSourceConfig, is_incremental=False) -> bool:
        if is_incremental:
            return True

        errors = []

        if not config.query:
            errors.append("Please specify the query in the source configuration.")

        if not config.host:
            errors.append("Please specify the host in the source configuration.")

        if not config.database:
            errors.append("Please specify the database in the source configuration.")

        if not config.user:
            errors.append("Please specify the user in the source configuration.")

        if not config.password:
            errors.append("Please specify the password in the source configuration.")

        if not config.port:
            errors.append("Please specify the port in the source configuration.")

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True


class MySqlSourceValidationPolicy(ValidationPolicy):
    def __init__(self):
        super().__init__()
        self.add(SourceValidator())
        self.add(MysqlSourceValidator())
        self.add(IncrementalSourceValidator())
