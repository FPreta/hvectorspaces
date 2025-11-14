import logging
import time
from tqdm import tqdm
from psycopg2 import OperationalError
from hvectorspaces.io.cockroach_client import CockroachClient

from dotenv import load_dotenv

# -------------------------
# FIXED DECADE RANGES
# -------------------------
DECADES = [(y, y + 9) for y in range(1920, 2030, 10)]
BATCH_SIZE = 2000  # safe for CockroachDB Serverless

logging.basicConfig(level=logging.ERROR)
load_dotenv()


def run_with_retries(client, fn, max_retries=5):
    """
    CockroachDB uses SERIALIZABLE isolation by default,
    so retries on TransactionRestart errors are required.
    """
    for attempt in range(max_retries):
        try:
            return client.run_transaction(fn)
        except OperationalError as e:
            msg = str(e)
            if "restart transaction" in msg.lower() or "retry txn" in msg.lower():
                wait = 0.5 * (attempt + 1)
                print(f"⚠️ Transaction restart required. Retrying in {wait:.1f}s...")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Exceeded max retries")


def update_decade(client, decade_start, decade_end):
    print(f"\n📘 Decade {decade_start}–{decade_end}")

    # Step 1: fetch all oa_ids in that decade
    def fetch_ids(cur):
        cur.execute(
            """
            SELECT oa_id
            FROM openalex_vector_spaces
            WHERE publication_year BETWEEN %s AND %s
        """,
            (decade_start, decade_end),
        )
        return cur.fetchall()

    ids = run_with_retries(client, fetch_ids)
    ids = [row[0] for row in ids]

    print(f"   → Found {len(ids)} works in this decade.")

    # Step 2: chunk the updates
    for i in tqdm(range(0, len(ids), BATCH_SIZE), desc=f"Updating {decade_start}s"):
        batch = ids[i : i + BATCH_SIZE]

        def apply_update(cur):
            cur.execute(
                """
                UPDATE openalex_vector_spaces AS o
                SET in_decade_references = COALESCE(refs.ref_array, ARRAY[]::STRING[])
                FROM (
                    SELECT
                        o.oa_id,
                        array_agg(ref) AS ref_array
                    FROM openalex_vector_spaces AS o
                    LEFT JOIN LATERAL (
                        SELECT ref
                        FROM unnest(o.referenced_works) AS ref
                        JOIN openalex_vector_spaces AS o2 ON o2.oa_id = ref
                        WHERE o2.publication_year BETWEEN %s AND %s
                    ) AS subq ON TRUE
                    WHERE o.oa_id = ANY(%s)
                    GROUP BY o.oa_id
                ) AS refs
                WHERE o.oa_id = refs.oa_id
            """,
                (decade_start, decade_end, batch),
            )

        try:
            run_with_retries(client, apply_update)
        except Exception as e:
            logging.error(
                f"❌ Failed on batch {i//BATCH_SIZE + 1} (IDs {i} to {min(i+BATCH_SIZE, len(ids))})"
            )
            raise e

    print(f"✅ Finished decade {decade_start}–{decade_end}")


def main():
    client = CockroachClient()

    print("\n🔧 Ensuring column exists...")
    client.run_transaction(
        lambda cur: cur.execute(
            """
        ALTER TABLE openalex_vector_spaces
        ADD COLUMN IF NOT EXISTS in_decade_references STRING[] DEFAULT ARRAY[]::STRING[];
    """
        )
    )

    for decade_start, decade_end in DECADES:
        update_decade(client, decade_start, decade_end)

    print("\n🎉 All decades completed.")


if __name__ == "__main__":
    main()
