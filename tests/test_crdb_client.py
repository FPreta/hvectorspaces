from dotenv import load_dotenv
import psycopg2
import pytest

from hvectorspaces.io.cockroach_client import CockroachClient

load_dotenv()


def test_cockroach_connection():
    """Test CockroachDB connection and basic operations."""
    with CockroachClient() as client:
        with client.conn.cursor() as cur:
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            assert result[0] == 1


def test_cockroach_tables():
    """Test CockroachDB table creation and deletion."""
    table_name = "test_table"
    sql_fields = {
        "id": "SERIAL PRIMARY KEY",
        "name": "STRING",
        "value": "INT",
    }
    with CockroachClient() as client:
        # Create table
        client.generate_table(table_name, sql_fields)
        with client.conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{table_name}');")
            result = cur.fetchone()
            assert result[0] == table_name

        # Drop table
        client.drop_table(table_name)
        with pytest.raises(psycopg2.Error):
            cur.execute(f"SELECT to_regclass('{table_name}');")


def test_cockroach_upload():
    """Test CockroachDB bulk upload functionality."""
    table_name = "test_table"
    sql_fields = {
        "id": "STRING",
        "name": "STRING",
        "value": "INT",
    }
    with CockroachClient() as client:
        client.generate_table(table_name, sql_fields)
        test_data = [
            {"id": 0, "name": "Alice", "value": 10},
            {"id": 1, "name": "Bob", "value": 20},
            {"id": 2, "name": "Charlie", "value": 30},
        ]
        client.upload_works(table_name, test_data, sql_fields)

        with client.conn.cursor() as cur:
            cur.execute(f"SELECT name, value FROM {table_name} ORDER BY id;")
            results = cur.fetchall()
            assert results == [("Alice", 10), ("Bob", 20), ("Charlie", 30)]

        client.drop_table(table_name)
