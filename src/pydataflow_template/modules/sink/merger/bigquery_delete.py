from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import List

logger = getLogger(__name__)


import apache_beam as beam
from apache_beam import window
from google.cloud import bigquery

TYPE_NUMBERS = [
    "INT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "TINYINT",
    "BYTEINT",
    "FLOAT64",
    "DECIMAL",
    "BIGDECIMAL",
]


class BatchKeys(beam.DoFn):
    def __init__(self, merge_keys: List[str], batch_size: int = 10000):
        self.merge_keys = merge_keys
        self.batch_size = batch_size
        self.batch = []

    def get_merge_keys(self, batch):
        ret = []
        for element in batch:
            ret.append({key: element[key] for key in self.merge_keys})
        return ret

    def process(self, element, **kwargs):
        self.batch.append(element)
        if len(self.batch) == self.batch_size:
            merge_keys = self.get_merge_keys(self.batch)
            yield merge_keys
            self.batch = []

    def finish_bundle(self, **kwargs):
        if self.batch:
            merge_keys = self.get_merge_keys(self.batch)
            # create WindowedValue object
            # window = beam.window.IntervalWindow(0, 10)
            # windowed_value = beam.window.WindowedValue(result, 0, [window])
            windowed_value = window.GlobalWindows().windowed_value(merge_keys, timestamp=0)
            yield windowed_value


class DeleteFromBigQuery(beam.DoFn):
    def __init__(self, table, schema, merge_keys, num_threads: int = 10, batch_size: int = 1):
        project, dataset_table = table.split(":")
        dataset, table = dataset_table.split(".")
        self.project = project
        self.dataset = dataset
        self.table = table
        self.schema = schema
        self.merge_keys = merge_keys
        self.num_threads = num_threads
        self.batch_size = batch_size

    def is_number_column(self, col):
        return col in [field.name for field in self.schema.fields if field.type in TYPE_NUMBERS]

    def start_bundle(self):
        self.client = bigquery.Client(self.project)

    def process(self, batch_of_keys):
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            for i in range(0, len(batch_of_keys), self.batch_size):
                batch = batch_of_keys[i : i + self.batch_size]
                executor.submit(self.fn, batch)

    def fn(self, batch):
        # Execute delete statement
        merge_keys_str = ",".join(self.merge_keys)
        merge_values_str = ""
        for i, merge_key_dict in enumerate(batch):
            if i != 0:
                merge_values_str += ","
            values = []
            for k, v in merge_key_dict.items():
                if self.is_number_column(k):
                    values.append(f"{v}")
                else:
                    values.append(f'"{v}"')
            merge_values_str += f"({','.join(values)})"

        query = f"DELETE FROM `{self.project}.{self.dataset}.{self.table}` WHERE ({merge_keys_str}) IN ({merge_values_str})"

        self.client.query(query)
        logger.info("Done delete query: %s", query)

        return batch
