import json
import os
import time
from tqdm import tqdm

from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

load_dotenv()


class CockroachClient:
    """Simple wrapper for CockroachDB using psycopg2."""

    def __init__(self):
        # Connect using standard Postgres env vars
        self.conn = psycopg2.connect(
            host=os.getenv("CRDB_HOST"),
            database=os.getenv("CRDB_DATABASE"),
            user=os.getenv("CRDB_USER"),
            password=os.getenv("CRDB_PASSWORD"),
            port=os.getenv("CRDB_PORT"),
            sslmode=os.getenv("CRDB_SSLMODE", "require"),
        )
        self.conn.autocommit = False

    # ---------------------------------------------------
    # Context management
    # ---------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()

    # ---------------------------------------------------
    # Resilient transaction decorator (Cockroach best practice)
    # ---------------------------------------------------
    def run_transaction(self, func, *args, max_retries=3, **kwargs):
        """
        Retry a transaction in case of serialization failure (SQLSTATE 40001).
        CockroachDB can abort concurrent transactions under contention.
        """
        for attempt in range(max_retries):
            try:
                with self.conn:
                    with self.conn.cursor() as cur:
                        return func(cur, *args, **kwargs)
            except psycopg2.Error as e:
                if e.pgcode == "40001":  # serialization failure
                    sleep = 2**attempt + (0.1 * attempt)
                    print(
                        f"Retrying transaction after serialization failure ({sleep:.1f}s)..."
                    )
                    time.sleep(sleep)
                    continue
                else:
                    raise
        raise RuntimeError("Max retries exceeded for transaction.")

    # ---------------------------------------------------
    # Schema management
    # ---------------------------------------------------
    def generate_table(self, table_name: str, fields: dict, pk: str = "id"):
        """Create table if not exists with given fields and types."""
        cols = []
        for name, ftype in fields.items():
            cols.append(sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(ftype)))

        # Only add a separate PRIMARY KEY constraint if not already in field definition
        pk_type = fields.get(pk, "").upper()
        if "PRIMARY KEY" not in pk_type:
            pk_constraint = sql.SQL(", PRIMARY KEY ({})").format(sql.Identifier(pk))
        else:
            pk_constraint = sql.SQL("")

        create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({}{});").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(cols),
            pk_constraint,
        )

        def _exec(cur):
            cur.execute(create_query)

        self.run_transaction(_exec)

    # ---------------------------------------------------
    # Bulk upload
    # ---------------------------------------------------
    def upload_works(self, table_name: str, works: list, fields: dict, pk: str = "id"):
        """
        Bulk upload a list of work dicts to CockroachDB with tqdm progress.

        Args:
            table_name (str): Target table name.
            works (list[dict]): List of records (each a dict).
            fields (dict): {field_name: sql_type} mapping.
        """
        if not works:
            return

        field_names = list(fields.keys())

        def convert_value(value, ftype):
            """Convert Python values based on declared SQL type."""
            if value is None:
                return None

            # Array types, e.g. TEXT[], STRING[]
            if ftype.endswith("[]"):
                if isinstance(value, list):
                    return value
                return [value]

            # JSON / JSONB types
            if ftype.upper() in ("JSONB", "JSON"):
                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                return json.dumps({"value": value})

            # Everything else: leave as-is
            return value

        # Prepare rows with correct conversions
        records = [
            [convert_value(work.get(fname), fields[fname]) for fname in field_names]
            for work in works
        ]

        insert_query = sql.SQL(
            """
            INSERT INTO {} ({})
            VALUES %s
            ON CONFLICT ({}) DO NOTHING;
        """
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, field_names)),
            sql.Identifier(pk),
        )

        page_size = 1000

        def _insert(cur):
            # Iterate over records in chunks with tqdm progress bar
            for i in tqdm(
                range(0, len(records), page_size),
                desc=f"Uploading {table_name}",
                unit="batch",
            ):
                batch = records[i : i + page_size]
                execute_values(
                    cur, insert_query.as_string(cur), batch, page_size=page_size
                )

        self.run_transaction(_insert)

    def drop_table(self, table_name: str, cascade: bool = False):
        """
        Drop a table from CockroachDB (useful for testing or resetting schema).

        Args:
            table_name (str): Name of the table to drop.
            cascade (bool): If True, also drop dependent objects (foreign keys, etc.).
        """
        drop_query = sql.SQL("DROP TABLE IF EXISTS {} {}").format(
            sql.Identifier(table_name), sql.SQL("CASCADE") if cascade else sql.SQL("")
        )

        with self.conn.cursor() as cur:
            cur.execute(drop_query)
        self.conn.commit()
        print(f"✅ Dropped table '{table_name}'{' (CASCADE)' if cascade else ''}.")
