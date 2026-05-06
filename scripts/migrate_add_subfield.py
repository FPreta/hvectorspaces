"""
Adds the 'subfield' column to openalex_vector_spaces and populates it
from openalex_vector_spaces_v2.csv.gz using a temp table + bulk UPDATE.
"""

import csv
import gzip
import io
import logging
import os

from dotenv import load_dotenv
from tqdm import tqdm

from hvectorspaces.io.pg_client import PostgresClient

csv.field_size_limit(10**7)

logging.basicConfig(level=logging.ERROR)
load_dotenv()

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "exported_db", "openalex_vector_spaces_v2.csv.gz")
TABLE = "openalex_vector_spaces"
BATCH_SIZE = 5000


def main():
    with PostgresClient() as client:
        print("Adding column if not present...")
        client.run_transaction(
            lambda cur: cur.execute(f"""
                ALTER TABLE {TABLE}
                ADD COLUMN IF NOT EXISTS subfield TEXT;
            """)
        )

        print("Reading subfield data from CSV...")
        pairs: list[tuple[str, str | None]] = []
        with gzip.open(CSV_PATH, "rt", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pairs.append((row["oa_id"], row["subfield"] or None))

        print(f"Loaded {len(pairs)} rows. Populating subfield in batches...")

        for i in tqdm(range(0, len(pairs), BATCH_SIZE), desc="Updating subfield", unit="batch"):
            batch = pairs[i : i + BATCH_SIZE]

            def apply_batch(cur, b=batch):
                buf = io.StringIO()
                writer = csv.writer(buf)
                for oa_id, subfield in b:
                    writer.writerow([oa_id, subfield if subfield is not None else ""])
                buf.seek(0)

                cur.execute("""
                    CREATE TEMP TABLE _subfield_update (oa_id TEXT, subfield TEXT)
                    ON COMMIT DROP;
                """)
                cur.copy_expert(
                    "COPY _subfield_update (oa_id, subfield) FROM STDIN WITH (FORMAT csv)",
                    buf,
                )
                cur.execute(f"""
                    UPDATE {TABLE} AS t
                    SET subfield = NULLIF(u.subfield, '')
                    FROM _subfield_update u
                    WHERE t.oa_id = u.oa_id;
                """)

            client.run_transaction(apply_batch)

        filled = sum(1 for _, sf in pairs if sf)
        print(f"Done. {filled}/{len(pairs)} rows have a subfield value.")


if __name__ == "__main__":
    main()
