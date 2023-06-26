import os

from dotenv import load_dotenv
from google.cloud import bigquery
import pytest


@pytest.fixture(scope="session")
def gcp_project():
    dotenv_path = os.path.join(os.path.dirname(__file__), "../../.env")
    load_dotenv(dotenv_path)
    project = os.getenv("GCP_PROJECT_ID")
    yield project


@pytest.fixture(scope="session")
def bq_client(gcp_project):
    client = bigquery.Client(project=gcp_project)
    yield client


@pytest.fixture
def create_bigquery_test_table(bq_client):
    dataset_id = "test"
    table_id = "test"

    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        bq_client.create_dataset(dataset)

    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("memo", "STRING", mode="NULLABLE"),
    ]

    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )

    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = time_partitioning

    bq_client.create_table(table)

    insert_query = f"""
        INSERT INTO `{bq_client.project}.{dataset_id}.{table_id}` (id, name, date, memo)
        VALUES
            (1, 'test data1', '2023-1-1', 'memo1'),
            (2, 'test data2', '2023-2-2', NULL),
            (3, 'test data3', '2023-3-3', 'memo3'),
            (4, 'test data4', '2023-4-4', NULL),
            (5, 'test data5', '2023-5-5', NULL)
    """
    bq_client.query(insert_query).result()

    try:
        yield

    finally:
        bq_client.delete_table(f"{bq_client.project}.{dataset_id}.{table_id}", not_found_ok=True)


@pytest.fixture
def create_bigquery_test_table_dest(bq_client):
    dataset_id = "test"
    table_id = "test_dest"

    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        bq_client.create_dataset(dataset)

    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("memo", "STRING", mode="NULLABLE"),
    ]

    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )

    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = time_partitioning

    bq_client.create_table(table)

    insert_query = f"""
        INSERT INTO `{bq_client.project}.{dataset_id}.{table_id}` (id, name, date, memo)
        VALUES
            (1, 'test data1', '2023-1-1', 'memo1'),
            (2, 'test data2', '2023-2-2', NULL),
            (3, 'test data3', '2023-3-3', 'memo3')
    """
    bq_client.query(insert_query).result()

    try:
        yield
    finally:
        bq_client.delete_table(f"{bq_client.project}.{dataset_id}.{table_id}", not_found_ok=True)
