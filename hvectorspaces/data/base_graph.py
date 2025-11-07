import aiohttp
import asyncio
import logging
from tqdm import tqdm

from hvectorspaces.data.deduplication import Deduper
from hvectorspaces.data.preprocessing import oa_id
from hvectorspaces.io.openalex_client import OpenAlexClient

logging.basicConfig(level=logging.INFO)


def build_seed(
    openalex_client: OpenAlexClient,
    search_term: str,
    min_citations: int,
    min_year_exclusive: int,
    select="id,doi,title,publication_year,cited_by_count,cited_by_api_url",
):
    filter_str = f"cited_by_count:>{min_citations},publication_year:>{min_year_exclusive}"
    d = Deduper()
    bar = tqdm(desc="Building seed", unit="work")
    for w in openalex_client.fetch_works_iter(
        search=search_term, filter_str=filter_str, select=select
    ):
        d.add(w)
        bar.update(1)
    bar.close()
    return d.kept


async def expand_batched(
    seed_works,
    oa_client: OpenAlexClient,
    hops=1,
    min_citations=20,
    year_gt=1920,
    select="id,doi,title,publication_year,cited_by_count,cited_by_api_url",
    delay_between_calls=0.0,
):
    d = Deduper()
    for w in seed_works:
        w["layer"] = 0
        d.add(w)

    seen_ids = {oa_id(w) for w in d.kept}
    frontier = list(seen_ids)
    layers = []

    async with aiohttp.ClientSession() as session:
        for hop in range(1, hops + 1):
            logging.info(f"\n--- Hop {hop} ---")
            citing_works = await oa_client.collect_citing_works(
                session, frontier, min_citations, year_gt, select
            )
            logging.info(f"Collected {len(citing_works)} citing works (raw)")

            new_this_hop = []
            for w in citing_works:
                w["layer"] = hop
                if d.add(w):
                    new_this_hop.append(w)

            new_ids = {oa_id(w) for w in new_this_hop}
            frontier = list(new_ids - seen_ids)
            seen_ids.update(new_ids)
            layers.append(new_this_hop)

            logging.info(
                f"Layer {hop}: {len(new_this_hop)} new works; total: {len(d.kept)}"
            )

            if not frontier:
                logging.info("No new frontier to expand.")
                break

            if delay_between_calls:
                await asyncio.sleep(delay_between_calls)

    return d.kept, layers
