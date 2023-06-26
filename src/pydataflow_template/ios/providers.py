import io
import json
from logging import getLogger
import re
from typing import Dict

from exceptions import InvalidPathError, IoError
from google.cloud import bigquery, storage
from ios.clients import MysqlClient, PostgresClient
from ios.schema import MetaData, Schema
from mysql.connector import FieldType
import pandas as pd
import psycopg2

logger = getLogger(__name__)


class LocalFileProvider:
    def read(self, file_path: str, **kargs):
        ext = file_path.split(".")[-1]
        if ext == "json":
            with open(file_path, "r", encoding="utf-8") as f:
                return json.loads(f.read())
                # return json.loads(f.read())
        elif ext == "sql":
            with open(file_path, encoding="utf-8") as f:
                return " ".join(f.read().split())
        elif ext in ["yaml", "yml"]:
            raise IoError("not supported ext.")
            # with open(file_path, 'r', encoding="utf-8") as f:
            #    return yaml.safe_load(f)
        else:
            raise IoError("not supported ext.")

    def get_metadata(self, **kargs):
        raise NotImplementedError()

    def write(self, **kargs):
        raise NotImplementedError()


class GcsFileProvider:
    def __init__(self, client: storage.Client):
        self.client = client

    def read(self, file_path: str, **kargs):
        if not file_path.startswith("gs://"):
            raise InvalidPathError(file_path)

        ext = file_path.split(".")[-1]
        if ext == "json":
            return json.loads(self._get_bytes(file_path))
        elif ext == "sql":
            return " ".join(self._get_bytes(file_path).decode("utf-8").split())
        elif ext in ["yaml", "yml"]:
            raise IoError("not supported ext.")
        else:
            raise IoError("not supported ext.")

    def _get_bytes(self, file_path):
        if m := re.match(r"gs:\/\/(.+?)\/", file_path):
            bucket_name = m.group(1)
        if m := re.match(r"gs:\/\/.+?\/(.+)$", file_path):
            blob_path = m.group(1)
        return self._download_blob_into_memory(bucket_name, blob_path)

    def _download_blob_into_memory(self, bucket_name, blob_path):
        blob = self.client.bucket(bucket_name).blob(blob_path)
        return blob.download_as_string()

    def get_metadata(self, **kargs):
        raise NotImplementedError()

    def write(self, **kargs):
        raise NotImplementedError()


class BigQueryRdbProvider:
    def __init__(self, client: bigquery.Client, schema: Schema):
        self.client = client
        self.schema = schema

    def read(self, query: str, **kargs) -> pd.DataFrame:
        logger.info("read query result from BigQuery. query: %s", query)
        query_job = self.client.query(query)
        result = query_job.result()
        columns = [field.name for field in result.schema]
        data = [list(row.values()) for row in result]
        return pd.DataFrame(data, columns=columns)
        # return self.client.query(query).to_dataframe()

    def get_metadata(self, query: str, **kargs):
        logger.info("get metadata of the query from BQ. query: %s", query)
        try:
            query_job = self.client.query(query)
            result = query_job.result()
            row_count = result.total_rows

            with io.StringIO("") as f:
                self.client.schema_to_json(result.schema, f)
                schema_dict = json.loads(f.getvalue())
                return MetaData(
                    schema=self.schema.from_dict(schema_dict, src_system="bigquery"),
                    row_count=row_count,
                )

        except Exception:
            msg = f"failed to get metadata of the query from BQ. query: {query}"
            logger.error(msg)
            raise IoError(msg)

    def write(self, **kargs):
        raise NotImplementedError()


class MysqlRdbProvider:
    def __init__(self, client: MysqlClient, schema: Schema):
        self.client = client
        self.schema = schema

    def read(self, config: Dict, query: str, **kargs):
        logger.info("read query result from mysql. query: %s", query)
        try:
            with self.client.get_cursor(config) as cur:
                cur.execute(query)
                return cur.fetchall()
        except Exception:
            msg = f"failed to read from mysql. query: {query}"
            logger.error(msg)
            raise IoError(msg)

    def get_metadata(self, config: Dict, query: str, **kargs):
        logger.info("get metadata of the query from mysql: %s", query)

        try:
            with self.client.get_cursor(config) as cur:
                cur.execute(query)
                row_count = cur.rowcount
                schema_dict = []
                for desc in cur.description:
                    schema_dict.append({"name": desc[0], "type": FieldType.get_info(desc[1])})
                return MetaData(
                    schema=self.schema.from_dict(schema_dict, src_system="mysql"),
                    row_count=row_count,
                )

        except Exception:
            msg = f"failed to get metadata of the query from mysql. query: {query}"
            logger.error(msg)
            raise IoError(msg)

    def write(self, **kargs):
        raise NotImplementedError()


class PostgresRdbProvider:
    def __init__(self, client: PostgresClient, schema: Schema):
        self.client = client
        self.schema = schema

    def read(self, config: Dict, query: str, **kargs):
        logger.info("read query result from postgres. query: %s", query)
        try:
            with self.client.get_cursor(config) as cur:
                cur.execute(query)
                return cur.fetchall()
        except Exception:
            msg = f"failed to read from postgres. query: {query}"
            logger.error(msg)
            raise IoError(msg)

    def get_metadata(self, config: Dict, query: str, **kargs):
        logger.info("get metadata of the query from postgres. query: %s", query)

        try:
            with self.client.get_cursor(config) as cur:
                cur.execute(query)
                row_count = cur.rowcount
                schema_dict = []
                for desc in cur.description:
                    schema_dict.append(
                        {
                            "name": desc.name,
                            "type": psycopg2.extensions.string_types[desc.type_code].name,
                        }
                    )
                return MetaData(
                    schema=self.schema.from_dict(schema_dict, src_system="psycopg2"),
                    row_count=row_count,
                )

        except Exception:
            msg = f"failed to get metadata of the query from postgres. query: {query}"
            logger.error(msg)
            raise IoError(msg)

    def write(self, **kargs):
        raise NotImplementedError()


class GcpKvsProvider:
    # def __init__(self, client):
    #    self.client = client  # GCP SecretManager client

    def read(self, **kargs):
        raise NotImplementedError()

    def get_metadata(self, **kargs):
        raise NotImplementedError()

    def write(self, **kargs):
        raise NotImplementedError()
