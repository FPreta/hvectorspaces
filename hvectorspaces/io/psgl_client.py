import gzip
import logging
import os
from typing import Any, Dict, List

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

load_dotenv()


class PostgresClient:
    """Simple wrapper around PostgreSQL using psycopg2."""

    def __init__(self):
        # Uses standard Postgres env vars
        # If you want explicit arguments, we can add them later.
        host = os.getenv("PG_HOST", "localhost")
        database = os.getenv("PG_DATABASE", "my_local_db")
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASSWORD")
        port = os.getenv("PG_PORT", "5432")
        if not user:
            raise ValueError("Environment variable PG_USER is not set. Please set PG_USER to specify the database user.")
        logging.info(
            f"Connecting to PostgreSQL with host='{host}', database='{database}', user='{user}', port='{port}'"
        )
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        self.conn.autocommit = False

    # ---------------------------------------------------
    # Context management
    # ---------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Transaction management is handled by run_transaction; just close the connection here.
        self.conn.close()

    # ---------------------------------------------------
    # Transactions (no retry logic needed for PostgreSQL)
    # ---------------------------------------------------
    def run_transaction(self, func, *args, **kwargs):
        """
        Run a function in a transaction.
        No retries, since PostgreSQL doesn't require CRDB-style retry loops.
        """
        with self.conn:
            with self.conn.cursor() as cur:
                return func(cur, *args, **kwargs)

    # ---------------------------------------------------
    # Schema utilities
    # ---------------------------------------------------
    def execute_sql(self, query: str, params=None):
        """Execute a generic SQL query and return results."""

        def _exec(cur):
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return None  # e.g., for CREATE TABLE

        return self.run_transaction(_exec)

    def generate_table(self, table_name: str, fields: Dict[str, str], pk="id"):
        """
        Create a PostgreSQL table if it does not exist.
        `fields` = {name: SQL type}
        """
        cols = [
            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(ftype))
            for name, ftype in fields.items()
        ]

        # Add PK if user specifies it and it's not already declared
        pk_defined = any("primary key" in f.lower() for f in fields.values())
        if isinstance(pk, str):
            pk = (pk,)

        if pk and not pk_defined:
            pk_clause = sql.SQL(", PRIMARY KEY ({})").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in pk)
            )
        else:
            pk_clause = sql.SQL("")

        query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({}{});").format(
            sql.Identifier(table_name), sql.SQL(", ").join(cols), pk_clause
        )

        def _exec(cur):
            cur.execute(query)

        self.run_transaction(_exec)

    def drop_table(self, table_name: str, cascade: bool = False):
        """Drop a table."""
        query = sql.SQL("DROP TABLE IF EXISTS {} {}").format(
            sql.Identifier(table_name), sql.SQL("CASCADE") if cascade else sql.SQL("")
        )

        def _exec(cur):
            cur.execute(query)

        self.run_transaction(_exec)
        logging.info(f"✓ Dropped table '{table_name}'")

    # ---------------------------------------------------
    # Metadata
    # ---------------------------------------------------
    def list_tables(self) -> List[str]:
        """List all public schema tables."""
        rows = self.execute_sql("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        return [r[0] for r in rows]

    def describe_table(self, table_name: str):
        """Return PostgreSQL column metadata."""
        query = sql.SQL("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s;
        """)

        def _exec(cur):
            cur.execute(query, (table_name,))
            return cur.fetchall()

        return self.run_transaction(_exec)

    # ---------------------------------------------------
    # CSV Loading (PostgreSQL COPY)
    # ---------------------------------------------------
    def load_csv(
        self, table_name: str, csv_path: str, has_header=True, gzip_compressed=False
    ):
        """
        Load data from CSV directly using PostgreSQL's COPY command.
        Assumes the CSV columns match the table columns exactly.
        """
        logging.info(f"📥 Loading {csv_path} → {table_name}")

        opener = gzip.open if gzip_compressed else open

        with opener(csv_path, "rt") as f:
            if has_header:
                header_line = f.readline().strip()
                columns = header_line.split(",")
            else:
                raise ValueError("CSV must have a header row to infer column order.")

            copy_sql = sql.SQL("""
                COPY {} ({}) FROM STDIN WITH (FORMAT csv)
            """).format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            )

            def _exec(cur):
                cur.copy_expert(copy_sql, f)

            self.run_transaction(_exec)

        logging.info(f"✓ Loaded {csv_path}")

    # ---------------------------------------------------
    # Bulk insert (for small data only)
    # ---------------------------------------------------
    def bulk_insert(self, table_name: str, rows: List[Dict[str, Any]]):
        """Simple INSERT for small data (not used for big imports)."""
        if not rows:
            return

        cols = list(rows[0].keys())
        query = sql.SQL("""
            INSERT INTO {} ({})
            VALUES ({})
        """).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(c) for c in cols),
            sql.SQL(", ").join(sql.Placeholder() for _ in cols),
        )

        def _exec(cur):
            for row in rows:
                cur.execute(query, list(row.values()))

        self.run_transaction(_exec)
        logging.info(f"✓ Inserted {len(rows)} rows into {table_name}")
