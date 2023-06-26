from datetime import date, datetime, time, timedelta
from decimal import Decimal

NO_MAPPINGS = {}

PYTHON_BIGQUERY_TYPE_MAP = {
    int: "INT64",
    float: "FLOAT64",
    str: "STRING",
    date: "DATE",
    datetime: "DATETIME",
}

PYTHON_PSYCOPG2_TYPE_MAP = {
    int: "INTEGER",
    float: "FLOAT",
    str: "TEXT",
    date: "DATE",
    datetime: "TIMESTAMP without time zone",
}

PYTHON_MYSQL_TYPE_MAP = {
    int: "INT",
    float: "FLOAT",
    str: "VARCHAR(255)",
    date: "DATE",
    datetime: "DATETIME",
}

BIGQUERY_PYTHON_TYPE_MAP = {
    "STRING": str,
    "INTEGER": int,
    "FLOAT": float,
    "BOOLEAN": bool,
    "TIMESTAMP": datetime,
    "DATE": date,
    "TIME": time,
    "DATETIME": datetime,
    "BYTES": bytes,
    "GEOGRAPHY": str,
}
BIGQUERY_PSYCOPG2_TYPE_MAP = {
    "STRING": "text",
    "INTEGER": "integer",
    "FLOAT": "real",
    "BOOLEAN": "boolean",
    "TIMESTAMP": "timestamp without time zone",
    "DATE": "date",
    "TIME": "time without time zone",
    "DATETIME": "timestamp without time zone",
    "BYTES": "bytea",
    "GEOGRAPHY": "geography",
}

BIGQUERY_MYSQL_TYPE_MAP = {
    "STRING": "VARCHAR(255)",
    "INTEGER": "INT",
    "FLOAT": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "DATETIME",
    "DATE": "DATE",
    "TIME": "TIME",
    "DATETIME": "DATETIME",
    "BYTES": "VARBINARY(255)",
    "GEOGRAPHY": "GEOMETRY",
}

PSYCOPG2_BIGQUERY_TYPE_MAP = {
    # Psycopg2 Type: BigQuery Type
    "smallint": "INTEGER",
    "integer": "INTEGER",
    "bigint": "INTEGER",
    "decimal": "NUMERIC",
    "numeric": "NUMERIC",
    "real": "FLOAT",
    "double precision": "FLOAT",
    "boolean": "BOOLEAN",
    "char": "STRING",
    "character": "STRING",
    "character varying": "STRING",
    "text": "STRING",
    "date": "DATE",
    "time with time zone": "TIME",
    "time without time zone": "TIME",
    "timestamp with time zone": "TIMESTAMP",
    "timestamp without time zone": "DATETIME",
    "interval": "STRING",
    "bytea": "BYTES",
    "binary": "BYTES",
    "uuid": "STRING",
    "json": "STRING",
    "jsonb": "STRING",
    "array": "RECORD",
    "geometry": "STRING",
    "geography": "GEOGRAPHY",
    "type": "STRING",
    "unknown": "STRING",
}


PSYCOPG2_PYTHON_TYPE_MAP = {
    "smallint": int,
    "integer": int,
    "bigint": int,
    "decimal": Decimal,
    "numeric": Decimal,
    "real": float,
    "double precision": float,
    "boolean": bool,
    "char": str,
    "character": str,
    "character varyinG": str,
    "text": str,
    "date": date,
    "time with time zone": time,
    "time without time zone": time,
    "timestamp with time zone": datetime,
    "timestamp without time zone": datetime,
    "interval": timedelta,
    "bytea": bytes,
    "json": dict,
    "jsonb": dict,
    "array": list,
    "geometry": str,
    "geography": str,
    "type": type,
    "unknown": None,
}

MYSQL_BIGQUERY_TYPE_MAP = {
    "DECIMAL": "NUMERIC",
    "TINY": "BOOL",
    "SHORT": "INT64",
    "LONG": "INT64",
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "NULL": "STRING",
    "TIMESTAMP": "DATETIME",
    "LONGLONG": "INT64",
    "INT24": "INT64",
    "DATE": "DATE",
    "TIME": "TIME",
    "DATETIME": "DATETIME",
    "YEAR": "INT64",
    "NEWDATE": "DATE",
    "VARCHAR": "STRING",
    "BIT": "INT64",
    "JSON": "STRING",
    "NEWDECIMAL": "NUMERIC",
    "ENUM": "STRING",
    "SET": "STRING",
    "TINY_BLOB": "BYTES",
    "MEDIUM_BLOB": "BYTES",
    "LONG_BLOB": "BYTES",
    "BLOB": "BYTES",
    "VAR_STRING": "STRING",
    "STRING": "STRING",
    "GEOMETRY": "STRING",
}

MYSQL_PYTHON_TYPE_MAP = {
    "DECIMAL": Decimal,
    "TINY": bool,
    "SHORT": int,
    "LONG": int,
    "FLOAT": float,
    "DOUBLE": float,
    "NULL": None,
    "TIMESTAMP": datetime,
    "LONGLONG": int,
    "INT24": int,
    "DATE": date,
    "TIME": time,
    "DATETIME": datetime,
    "YEAR": int,
    "NEWDATE": date,
    "VARCHAR": str,
    "BIT": int,
    "JSON": str,
    "NEWDECIMAL": Decimal,
    "ENUM": str,
    "SET": str,
    "TINY_BLOB": bytes,
    "MEDIUM_BLOB": bytes,
    "LONG_BLOB": bytes,
    "BLOB": bytes,
    "VAR_STRING": str,
    "STRING": str,
    "GEOMETRY": str,
}
