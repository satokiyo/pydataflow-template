import copy
import logging
import re
from typing import Any, Dict, Tuple, Union

from configs.config_base import ModuleConfigBase
from configs.sink.sink_config_base import (
    IncrementalSinkConfigBase,
    IncrementalSinkValidator,
    SinkConfigCommonParams,
    SinkValidator,
)
from configs.validator_base import ValidationPolicy, Validator
from exceptions import ParameterValidationError
from ios.ios import IoType
from ios.schema import Schema
import pytz

logger = logging.getLogger(__name__)


class BigQuerySinkConfigParams(SinkConfigCommonParams):
    # Parameters specific to the BigQuery sink module.
    table: str = ""
    schema: Union[str, Schema, None] = "auto"
    create_disposition: str = "CREATE_NEVER"
    partitioning: str = ""
    partitioning_field: str = ""
    clustering: str = ""
    gcs_temp_location: str = ""


class BigQuerySinkConfig(ModuleConfigBase, IncrementalSinkConfigBase, BigQuerySinkConfigParams):
    REGEXP_SCHEMA = re.compile(r"([a-zA-Z0-9_]+:[a-zA-Z]+,?)+[^,\s]+$")

    def from_dict(self, **param_dict):
        copied = copy.deepcopy(param_dict)  # deep copy for update
        schema_param = copied.get("parameters").get("schema", None)

        if not schema_param:
            schema_param = "auto"

        if schema_param in ["auto", "get_from_table"]:
            return super().from_dict(**param_dict)

        if isinstance(schema_param, Schema):
            return super().from_dict(**param_dict)

        if schema_param.endswith(".json"):
            # If a JSON file path is specified
            if schema_param.startswith("gs://"):
                schema_dict = self.io_adapter.read(IoType.GCS_FILE_IO, file_path=schema_param)
            else:
                schema_dict = self.io_adapter.read(IoType.LOCAL_FILE_IO, file_path=schema_param)
            schema_ = Schema().from_dict(schema_dict, src_system="bigquery")
        else:
            # Example: 'id:INTEGER,change_id:INTEGER,contract_id:INTEGER,...'
            if not self.REGEXP_SCHEMA.match(schema_param):
                msg = "Schema format is invalid."
                logger.error(msg)
                raise ParameterValidationError(msg)
            schema_ = Schema().from_str(schema_param, src_system="bigquery")

        copied.get("parameters").update({"schema": schema_})
        updated = copied
        return super().update_module_params(**updated)

    def get_incremental_interval_from_destination_table(
        self,
        incremental_column: str,
        io_type: IoType,
        timezone: pytz.timezone,
    ) -> Tuple[str, str]:
        # Retrieve schema from the BigQuery table
        table_ref = self.table
        _, table = table_ref.split(":")
        stmt = f"select * from {table}"
        destination_table_schema = self.io_adapter.get_metadata(
            io_type=IoType.BIGQUERY_RDB_IO, query=stmt
        ).schema
        if incremental_column not in [field.name for field in destination_table_schema.fields]:
            msg = "The specified incremental_column for incremental mode was not found in the destination table."
            logger.error(msg)
            raise ParameterValidationError(msg)

        # Get the data type of the incremental_column
        column_data_type: str = ""
        for field in destination_table_schema.fields:
            if field.name != incremental_column:
                continue
            column_data_type = field.type
            break
        if column_data_type.upper() not in ["DATE", "DATETIME", "TIMESTAMP"]:
            msg = "Invalid target type."
            logger.error(msg)
            raise ParameterValidationError(msg)

        # Get the MAX value of the incremental_column from the sink table
        _, table = self.table.split(":")
        if column_data_type.upper() == "DATE":
            query = (
                f"SELECT FORMAT_DATE('%Y-%m-%d 23:59:59', MAX({incremental_column})) FROM {table}"
            )
        elif column_data_type.upper() == "DATETIME":
            query = f"SELECT FORMAT_DATETIME('%Y-%m-%d %H-%M-%S', MAX({incremental_column})) FROM {table}"
        elif column_data_type.upper() == "TIMESTAMP":
            query = f"SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H-%M-%S', MAX({incremental_column}), {timezone.zone}) FROM {table}"

        logger.debug(query)

        # Execute the query
        df_query_result = self.io_adapter.read(io_type=io_type, query=query)

        if df_query_result.empty:
            msg = "The query to retrieve the previous MAX value from the table specified in destination_sink_name failed. The table may be empty."
            logger.error(msg)
            raise ParameterValidationError(msg)

        # Set the retrieved MAX value as incremental_interval_from
        incremental_interval_from = str(df_query_result.values[0, 0])

        return incremental_interval_from, column_data_type

    def get_additional_bq_parameters(self) -> Dict[str, Any]:
        """
        Get additional parameters for BigQuery.

        Returns:
            Dictionary containing additional BigQuery parameters.
        """
        additional_bq_parameters: Dict[str, Any] = {}

        if self.partitioning and self.partitioning_field:
            additional_bq_parameters.update(
                {
                    "timePartitioning": {
                        "type": self.partitioning.upper(),
                        "field": self.partitioning_field,
                    },
                }
            )
        if self.clustering:
            additional_bq_parameters.update(
                {"clustering": {"fields": [field.strip() for field in self.clustering.split(",")]}}
            )
        return additional_bq_parameters


class BigQuerySinkValidator(Validator):
    REGEXP_TABLE = re.compile(r"(.*):(.*).(.*)")
    REGEXP_CLUSTERING = re.compile(r"([a-zA-Z0-9_]+,?)+[^,\s]+$")

    def validate(self, config: BigQuerySinkConfig, is_incremental=False) -> bool:
        if is_incremental:
            return True

        errors = []

        if not config.table:
            errors.append("Required param table is not specified.")

        if not self.REGEXP_TABLE.match(config.table):
            errors.append("Table format is invalid. The format should be project_id:dataset.table")

        if config.create_disposition not in ["CREATE_IF_NEEDED", "CREATE_NEVER"]:
            errors.append("Specified create_disposition is invalid.")

        if config.partitioning:
            if config.partitioning.upper() not in ["HOUR", "DAY", "MONTH", "YEAR"]:
                errors.append("Specified partitioning is invalid.")

            if not config.partitioning_field:
                errors.append("Specified partitioning, but not partitioning_field.")

        if isinstance(config.schema, Schema) and config.partitioning:
            fields_dict = {field.name: field.type for field in config.schema.fields}
            partitioning_field_type = fields_dict.get(config.partitioning_field, None)

            if not partitioning_field_type:
                errors.append("partitioning_field not in target table schema")

            elif partitioning_field_type.upper() not in [
                "TIMESTAMP",
                "DATE",
                "DATETIME",
            ]:
                errors.append(
                    "partitioning field can only be of type `TIMESTAMP`, `DATE` or `DATETIME`"
                )

        if config.clustering:
            if not self.REGEXP_CLUSTERING.match(config.clustering):
                errors.append("clustering format is invalid.")

        if isinstance(config.schema, Schema) and config.clustering:
            cnt = 0
            for clustering_field in config.clustering.split(","):
                if fields_dict.get(clustering_field, None):
                    cnt += 1
            if cnt == 0:
                logger.warn("clustering columns not found in target schema.")  # not raise error
                pass

            if cnt > 4:
                errors.append("too many clustering columns.")

        if errors:
            logger.error("\n".join(errors))
            raise ParameterValidationError("\n".join(errors))

        return True


class IncrementalBigQuerySinkValidator(Validator):
    def validate(self, config: BigQuerySinkConfig, is_incremental=False) -> bool:
        """
        Validate the BigQuerySinkConfig parameters for incremental mode.

        Args:
            config: BigQuerySinkConfig instance to be validated.
            is_incremental: Boolean indicating if the validation is for an incremental sink.

        Returns:
            Boolean indicating if the validation passed.
        """
        if not is_incremental:
            return True

        if not config.partitioning_field:
            msg = "partitioning_field is not specified in sink config in incremental mode."
            logger.error(msg)
            raise ParameterValidationError(msg)
        if not config.partitioning:
            msg = "partitioning is not specified in sink config in incremental mode. partitioning specification is required."
            logger.error(msg)
            raise ParameterValidationError(msg)
        if config.create_disposition == "CREATE_IF_NEEDED":
            msg = "Sink table must already be exist in incremental mode. You can not specify `CREATE_IF_NEEDED`."
            logger.error(msg)
            raise ParameterValidationError(msg)
        return True


class BigQuerySinkValidationPolicy(ValidationPolicy):
    def __init__(self):
        super().__init__()
        self.add(SinkValidator())
        self.add(BigQuerySinkValidator())
        self.add(IncrementalSinkValidator())
        self.add(IncrementalBigQuerySinkValidator())
