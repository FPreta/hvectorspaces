import asyncio
import csv
import gzip
import json
import os

from hvectorspaces.data.base_graph import build_seed, expand_batched
from hvectorspaces.io.openalex_client import OpenAlexClient

SEED_FIELDS = [
    "oa_id", "doi", "title", "publication_year", "cited_by_count",
    "abstract", "referenced_works", "domain", "field", "subfield", "topic",
]


def _save_seed_checkpoint(works, path):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SEED_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for w in works:
            row = {
                k: json.dumps(w[k]) if isinstance(w.get(k), (list, dict)) else w.get(k)
                for k in SEED_FIELDS
            }
            writer.writerow(row)
    print(f"Seed checkpoint saved: {path} ({len(works)} works)")


def _load_seed_checkpoint(path):
    works = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            w = dict(row)
            w["referenced_works"] = json.loads(w["referenced_works"]) if w.get("referenced_works") else []
            w["publication_year"] = int(w["publication_year"]) if w.get("publication_year") else None
            w["cited_by_count"] = int(w["cited_by_count"]) if w.get("cited_by_count") else None
            works.append(w)
    print(f"Loaded seed from checkpoint: {path} ({len(works)} works)")
    return works


if __name__ == "__main__":
    oa_fields = [
        "id",
        "doi",
        "title",
        "publication_year",
        "cited_by_count",
        "abstract_inverted_index",
        "referenced_works",
        "primary_topic",
    ]
    sql_fields = {
        "oa_id": "TEXT",
        "doi": "TEXT",
        "title": "TEXT",
        "publication_year": "INT",
        "cited_by_count": "INT",
        "abstract": "TEXT",
        "referenced_works": "JSONB",
        "domain": "TEXT",
        "field": "TEXT",
        "subfield": "TEXT",
        "topic": "TEXT",
        "layer": "INT",
    }
    table_name = "openalex_vector_spaces"
    out_path = "exported_db/v2/openalex_vector_spaces.csv.gz"
    seed_checkpoint = "exported_db/v2/seed_checkpoint.csv.gz"

    select_str = ",".join(oa_fields)
    oa_client = OpenAlexClient()

    if os.path.exists(seed_checkpoint):
        seed_works = _load_seed_checkpoint(seed_checkpoint)
    else:
        seed_works = build_seed(
            openalex_client=oa_client,
            search_term="vector space",
            min_citations=20,
            min_year_exclusive=1920,
            select=select_str,
        )
        _save_seed_checkpoint(seed_works, seed_checkpoint)

    all_works, _ = asyncio.run(
        expand_batched(
            seed_works=seed_works,
            oa_client=oa_client,
            hops=2,
            min_citations=20,
            year_gt=1920,
            select=select_str,
            delay_between_calls=0.2,
        )
    )

    field_names = list(sql_fields.keys())
    with gzip.open(out_path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(field_names)
        for w in all_works:
            row = []
            for fname in field_names:
                val = w.get(fname)
                if isinstance(val, (list, dict)):
                    row.append(json.dumps(val))
                else:
                    row.append(val)
            writer.writerow(row)

    print(f"Saved {len(all_works)} works to {out_path}")
