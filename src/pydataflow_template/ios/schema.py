from dataclasses import dataclass
import logging
from typing import Dict, List, Type

from ios.converter import (
    BIGQUERY_MYSQL_TYPE_MAP,
    BIGQUERY_PSYCOPG2_TYPE_MAP,
    BIGQUERY_PYTHON_TYPE_MAP,
    MYSQL_BIGQUERY_TYPE_MAP,
    MYSQL_PYTHON_TYPE_MAP,
    NO_MAPPINGS,
    PSYCOPG2_BIGQUERY_TYPE_MAP,
    PSYCOPG2_PYTHON_TYPE_MAP,
    PYTHON_BIGQUERY_TYPE_MAP,
    PYTHON_MYSQL_TYPE_MAP,
    PYTHON_PSYCOPG2_TYPE_MAP,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Field:
    """
    Represents a field in a schema.
    """

    name: str
    type: str
    src_system: str  # bigquery, mysql, postgres ...etc.


class FieldConverter:
    @classmethod
    def convert(cls, field: Field, target_system: str) -> Field:
        name = field.name
        type = field.type
        mapping_dict = cls.get_mapping_dict(src=field.src_system, target=target_system)
        if mapping_dict == NO_MAPPINGS:
            return Field(name, type, src_system=target_system)
        converted_type = mapping_dict[type]
        return Field(name, converted_type, src_system=target_system)

    @classmethod
    def get_mapping_dict(cls, src: str, target: str):
        if src == target:
            return NO_MAPPINGS
        if src == "psycopg2" and target == "bigquery":
            return PSYCOPG2_BIGQUERY_TYPE_MAP
        if src == "bigquery" and target == "psycopg2":
            return BIGQUERY_PSYCOPG2_TYPE_MAP
        if src == "bigquery" and target == "python":
            return BIGQUERY_PYTHON_TYPE_MAP
        if src == "python" and target == "bigquery":
            return PYTHON_BIGQUERY_TYPE_MAP
        if src == "psycopg2" and target == "python":
            return PSYCOPG2_PYTHON_TYPE_MAP
        if src == "python" and target == "psycopg2":
            return PYTHON_PSYCOPG2_TYPE_MAP
        if src == "mysql" and target == "bigquery":
            return MYSQL_BIGQUERY_TYPE_MAP
        if src == "bigquery" and target == "mysql":
            return BIGQUERY_MYSQL_TYPE_MAP
        if src == "mysql" and target == "python":
            return MYSQL_PYTHON_TYPE_MAP
        if src == "python" and target == "mysql":
            return PYTHON_MYSQL_TYPE_MAP
        raise NotImplementedError("Mappings not defined.")


class Schema:
    """
    Interface class for schemas.
    """

    def __init__(self):
        self.fields: List[Field] = list()

    def from_dict(self, schema_dict: dict, src_system: str) -> Type["Schema"]:
        """
        Creates a Schema object from a dictionary.
        """
        for record in schema_dict:
            self.fields.append(Field(record["name"], record["type"], src_system))
        return self

    def from_str(self, schema_str: str, src_system: str) -> Type["Schema"]:
        """
        Creates a Schema object from a string.
        """
        for elem in schema_str.split(","):
            name, type = elem.split(":")
            self.fields.append(Field(name, type, src_system))
        return self

    def to_json(self, target_system: str) -> List[Dict[str, str]]:
        """
        Converts the Schema object to json.
        """
        converted_fields = [
            FieldConverter.convert(field, target_system=target_system) for field in self.fields
        ]
        return [{"name": f.name, "type": f.type} for f in converted_fields]


@dataclass(frozen=True)
class MetaData:
    schema: Schema
    row_count: int
