import os

import pytest
from dotenv import load_dotenv

from hvectorspaces.io.pg_client import PostgresClient

load_dotenv()


@pytest.fixture
def test_data():
    return [
        {"id": 0, "name": "Alice", "value": 10},
        {"id": 1, "name": "Bob", "value": 20},
        {"id": 2, "name": "Charlie", "value": 30},
    ]


@pytest.fixture
def sql_fields():
    return {
        "id": "TEXT",
        "name": "TEXT",
        "value": "INT",
    }


@pytest.fixture
def table_name():
    return "test_table"


def test_pg_connection():
    """Test PostgreSQL connection and basic operations."""
    with PostgresClient() as client:
        with client.conn.cursor() as cur:
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            assert result[0] == 1


def test_pg_tables(table_name):
    """Test PostgreSQL table creation and deletion."""
    sql_fields = {
        "id": "SERIAL PRIMARY KEY",
        "name": "TEXT",
        "value": "INT",
    }
    with PostgresClient() as client:
        # Create table
        client.generate_table(table_name, sql_fields)
        with client.conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{table_name}');")
            result = cur.fetchone()
            assert result[0] == table_name

        # Drop table
        client.drop_table(table_name)
        with client.conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{table_name}');")
            result = cur.fetchone()
            assert result[0] is None  # table no longer exists


def test_postgresql_upload(table_name, test_data, sql_fields):
    """Test PostgreSQL bulk upload functionality."""
    with PostgresClient() as client:
        client.generate_table(table_name, sql_fields)
        client.upload_works(table_name, test_data, sql_fields)

        with client.conn.cursor() as cur:
            cur.execute(f"SELECT name, value FROM {table_name} ORDER BY id;")
            results = cur.fetchall()
            assert results == [("Alice", 10), ("Bob", 20), ("Charlie", 30)]

        client.drop_table(table_name)


def test_csv_dump_and_load(table_name, test_data, sql_fields):
    """Test PostgreSQL loading and dumping to/from csv"""
    with PostgresClient() as client:
        client.generate_table(table_name, sql_fields)
        client.upload_works(table_name, test_data, sql_fields)
        client.export_table_to_csv(table_name, "tests/data/test.csv")
        client.drop_table(table_name)

    with PostgresClient() as client:
        client.generate_table(table_name, sql_fields)
        client.load_csv(table_name, "tests/data/test.csv")

        with client.conn.cursor() as cur:
            cur.execute(f"SELECT name, value FROM {table_name} ORDER BY id;")
            results = cur.fetchall()
            assert results == [("Alice", 10), ("Bob", 20), ("Charlie", 30)]

    os.remove("tests/data/test.csv")


def test_fetch_table_schema(sql_fields, table_name):
    with PostgresClient() as client:
        client.generate_table(table_name, sql_fields)
        table_schema = client.fetch_table_schema(table_name)
        assert (
            table_schema
            == """CREATE TABLE test_table (
  id text NOT NULL,
  name text,
  value integer,
  PRIMARY KEY (id)
)"""
        )


def test_fetch_in_decade_references():
    """Test fetching works from a specific decade with their in-decade references."""
    decade_start = 1970
    with PostgresClient() as client:
        results = client.fetch_per_decade_data(
            decade_start, additional_fields=["publication_year", "referenced_works"]
        )
        results = list(results)
        assert len(results) > 0
        oa_ids = {row[0] for row in results}
        assert any(in_dec_ref for _, in_dec_ref, _, _ in results)
        for row in results:
            oa_id, in_decade_references, publication_year, referenced_works = row
            assert isinstance(oa_id, str)
            assert all(ref in oa_ids for ref in in_decade_references)
            assert (
                set(referenced_works)
                .intersection(oa_ids)
                .issubset(set(in_decade_references))
            )
            assert 1970 <= publication_year <= 1979
