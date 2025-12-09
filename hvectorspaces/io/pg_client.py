import csv
import gzip
import json
import logging
import os
from typing import Any, Dict, List

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from tqdm import tqdm

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
            raise ValueError(
                "Environment variable PG_USER is not set. Please set PG_USER to specify the database user."
            )
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

    def upload_works(self, table_name: str, works: list, fields: dict):
        """
        Ultra-fast bulk load using PostgreSQL COPY FROM STDIN.
        This bypasses INSERT entirely.
        """

        if not works:
            return

        field_names = list(fields.keys())

        # Serialize rows into CSV-formatted text lines
        def serialize_row(work):
            row = []
            for fname in field_names:
                val = work.get(fname)

                # JSON
                if fields[fname].upper() in ("JSON", "JSONB"):
                    row.append(json.dumps(val) if val is not None else "")

                # Arrays → JSON string
                elif fields[fname].endswith("[]"):
                    row.append(json.dumps(val) if val is not None else "[]")

                else:
                    row.append(val)
            return row

        rows = [serialize_row(w) for w in works]

        # Build COPY … FROM STDIN
        copy_sql = sql.SQL("""
            COPY {} ({})
            FROM STDIN WITH (FORMAT csv)
        """).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(c) for c in field_names),
        )

        def _copy(cur):
            # Write CSV to PostgreSQL’s stdin stream
            from io import StringIO

            buffer = StringIO()
            writer = csv.writer(buffer)

            for row in rows:
                writer.writerow(row)

            buffer.seek(0)
            cur.copy_expert(copy_sql, buffer)

        self.run_transaction(_copy)

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
                reader = csv.reader([header_line])
                columns = next(reader)
            else:
                raise ValueError("CSV must have a header row to infer column order.")

        copy_sql = sql.SQL("""
            COPY {} ({}) FROM STDIN WITH (FORMAT csv)
        """).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        )

        def _exec(cur):
            with opener(csv_path, "rt") as f:
                next(f)  # skip header
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

    def export_table_to_csv(self, table_name: str, out_path: str, batch_size=10000):
        logging.info(f"Exporting {table_name} → {out_path}")

        # get columns
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
            colnames = [desc[0] for desc in cur.description]

        # detect JSON columns (Postgres doesn't return arrays as TEXT)
        type_info = self.execute_sql(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
        """,
            (table_name,),
        )
        json_cols = {col for col, typ in type_info if typ in ("json", "jsonb")}

        # Count rows
        total = self.execute_sql(f"SELECT count(*) FROM {table_name}")[0][0]

        pbar = tqdm(total=total, desc=f"Exporting {table_name}")

        offset = 0
        opener = gzip.open if out_path.endswith(".gz") else open
        with opener(out_path, "wt", encoding="utf-8", newline="") as gz:
            writer = csv.writer(gz)
            writer.writerow(colnames)

            while True:
                rows = self.execute_sql(
                    f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {batch_size} OFFSET {offset}"
                )
                if not rows:
                    break

                for row in rows:
                    newrow = []
                    for col, value in zip(colnames, row):
                        if col in json_cols and value is not None:
                            newrow.append(json.dumps(value))
                        else:
                            newrow.append(value)
                    writer.writerow(newrow)

                offset += batch_size
                pbar.update(len(rows))

        logging.info(f"✓ Export complete: {out_path}")

    def fetch_table_schema(self, table_name: str) -> str:
        """
        PostgreSQL does not support SHOW CREATE TABLE.
        We reconstruct CREATE TABLE from pg_catalog.
        """

        # Fetch columns
        columns = self.execute_sql(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """,
            (table_name,),
        )

        # Fetch PK
        pk_rows = self.execute_sql(
            """
            SELECT a.attname
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                   AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = %s::regclass
            AND    i.indisprimary;
        """,
            (table_name,),
        )
        pk_cols = [r[0] for r in pk_rows]

        # Build CREATE TABLE
        col_defs = []
        for name, dtype, nullable, default in columns:
            line = f"{name} {dtype}"
            if default:
                line += f" DEFAULT {default}"
            if nullable == "NO":
                line += " NOT NULL"
            col_defs.append(line)

        if pk_cols:
            col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

        ddl = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(col_defs) + "\n)"

        return ddl

    # ------------------------------------------------------------------
    # Add: export_all
    # ------------------------------------------------------------------
    def export_all(self, out_dir="exported_postgres"):
        os.makedirs(out_dir, exist_ok=True)

        tables = self.list_tables()

        # Write schema
        schema_path = os.path.join(out_dir, "schema.sql")
        with open(schema_path, "w") as f:
            for table in tables:
                ddl = self.fetch_table_schema(table)
                f.write(ddl + ";\n\n")

        # Export data
        for table in tables:
            out = os.path.join(out_dir, f"{table}.csv.gz")
            self.export_table_to_csv(table, out)

        logging.info(f"🎉 Full export complete → {out_dir}")

    def fetch_per_decade_data(
        self, decade_start: int, additional_fields: list[str] | None = None
    ):
        if decade_start % 10 != 0:
            raise ValueError("decade_start must be a multiple of 10.")

        decade_end = decade_start + 9

        fields = [sql.Identifier("oa_id"), sql.Identifier("in_decade_references")]
        if additional_fields:
            fields.extend(sql.Identifier(f) for f in additional_fields)

        query = sql.SQL("""
            SELECT {fields}
            FROM openalex_vector_spaces
            WHERE publication_year BETWEEN %s AND %s
        """).format(fields=sql.SQL(", ").join(fields))

        def _exec(cur):
            cur.execute(query, (decade_start, decade_end))
            return cur.fetchall()

        return self.run_transaction(_exec)
