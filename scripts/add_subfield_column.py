"""
Fetches primary_topic.subfield for all works in openalex_vector_spaces.csv.gz
and writes an updated file with a new 'subfield' column.
"""

import csv
import gzip
import io
import os
import sys

from tqdm import tqdm

from hvectorspaces.io.openalex_client import OpenAlexClient
from hvectorspaces.utils.iter_utils import chunked

csv.field_size_limit(10**7)

INPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "exported_db", "openalex_vector_spaces.csv.gz")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "exported_db", "openalex_vector_spaces_v2.csv.gz")

BATCH_SIZE = 100


def fetch_subfields(oa_ids: list[str], client: OpenAlexClient) -> dict[str, str | None]:
    """Returns a mapping of oa_id -> subfield display_name (or None)."""
    subfield_map: dict[str, str | None] = {}
    batches = list(chunked(oa_ids, BATCH_SIZE))
    for chunk in tqdm(batches, desc="Fetching subfields", unit="batch"):
        works = client.fetch_works_by_ids(chunk, batch_size=BATCH_SIZE, sleep=0.2, select="id,primary_topic")
        for w in works:
            wid = w.get("id", "").split("/")[-1]
            pt = w.get("primary_topic") or {}
            sf = pt.get("subfield") or {}
            subfield_map[wid] = sf.get("display_name") or None
    return subfield_map


def main():
    print("Reading existing CSV…")
    with gzip.open(INPUT_PATH, "rt", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if "subfield" in fieldnames:
        print("Column 'subfield' already present — nothing to do.")
        sys.exit(0)

    all_ids = [r["oa_id"] for r in rows]
    print(f"Total works: {len(all_ids)}")

    client = OpenAlexClient()
    subfield_map = fetch_subfields(all_ids, client)

    print("Writing updated CSV…")
    new_fieldnames = list(fieldnames)
    topic_idx = new_fieldnames.index("topic")
    new_fieldnames.insert(topic_idx + 1, "subfield")

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        wrapper = io.TextIOWrapper(gz, newline="")
        writer = csv.DictWriter(wrapper, fieldnames=new_fieldnames)
        writer.writeheader()
        for row in rows:
            row["subfield"] = subfield_map.get(row["oa_id"])
            writer.writerow(row)
        wrapper.flush()

    with open(OUTPUT_PATH, "wb") as f:
        f.write(buf.getvalue())

    filled = sum(1 for v in subfield_map.values() if v)
    print(f"Done. {filled}/{len(all_ids)} works have a subfield.")
    print(f"Written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
