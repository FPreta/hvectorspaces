import os

from dotenv import load_dotenv
from psycopg2 import sql

from hvectorspaces.io.pg_client import PostgresClient

load_dotenv()

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "exported_db", "schema.sql")

OA_CSV = os.path.join(os.path.dirname(__file__), "..", "exported_db", "openalex_vector_spaces.csv.gz")
CITATION_CSV = os.path.join(os.path.dirname(__file__), "..", "exported_db", "per_decade_citation_graph.csv.gz")


def main():
    with PostgresClient() as client:
        print("Dropping existing tables...")
        for table in ("per_decade_citation_graph", "openalex_vector_spaces"):
            client.run_transaction(
                lambda cur, t=table: cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(t))
                )
            )

        print("Creating schema...")
        with open(SCHEMA_PATH) as f:
            schema_sql = f.read()
        client.run_transaction(lambda cur: cur.execute(schema_sql))

        print("Loading openalex_vector_spaces...")
        client.load_csv(
            table_name="openalex_vector_spaces",
            csv_path=OA_CSV,
            gzip_compressed=True,
        )

        print("Loading per_decade_citation_graph...")
        client.load_csv(
            table_name="per_decade_citation_graph",
            csv_path=CITATION_CSV,
            gzip_compressed=True,
        )

        print("Done.")


if __name__ == "__main__":
    main()
