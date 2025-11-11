import asyncio

from hvectorspaces.data.base_graph import build_seed, expand_batched
from hvectorspaces.io.openalex_client import OpenAlexClient
from hvectorspaces.io.cockroach_client import CockroachClient

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
        "oa_id": "STRING",
        "doi": "STRING",
        "title": "STRING",
        "publication_year": "INT",
        "cited_by_count": "INT",
        "abstract": "STRING",
        "referenced_works": "STRING[]",
        "domain": "TEXT",
        "field": "TEXT",
        "topic": "TEXT",
        "layer": "INT",
    }
    table_name = "openalex_vector_spaces"

    select_str = ",".join(oa_fields)
    oa_client = OpenAlexClient()

    seed_works = build_seed(
        openalex_client=oa_client,
        search_term="vector space",
        min_citations=20,
        min_year_exclusive=1920,
        select=select_str,
    )

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
    with CockroachClient() as cr_client:
        cr_client.generate_table(table_name, sql_fields, pk="oa_id")
        cr_client.upload_works(table_name, all_works, sql_fields, pk="oa_id")
