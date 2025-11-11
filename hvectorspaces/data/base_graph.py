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
    select: str = "id,doi,title,publication_year,cited_by_count,cited_by_api_url",
) -> list:
    """Generates a seed set of works from OpenAlex based on a search term and filters.

    Args:
        openalex_client (OpenAlexClient): An instance of OpenAlexClient to fetch works.
        search_term (str): The search term to query works.
        min_citations (int): Minimum number of citations a work must have to be included.
        min_year_exclusive (int): Minimum publication year (exclusive) for works to be included.
        select (str, optional): Fields to return. Defaults to "id,doi,title,publication_year,cited_by_count,cited_by_api_url".

    Returns:
        list: A deduplicated list of works matching the criteria.
    """
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
    hops: int = 1,
    min_citations: int = 20,
    year_gt: int = 1920,
    select: str = "id,doi,title,publication_year,cited_by_count,cited_by_api_url",
    delay_between_calls: float = 0.0,
) -> tuple[list, list]:
    """Expands a seed set of works by collecting citing works in hops.
    Args:
        seed_works (list): Initial list of works to expand from.
        oa_client (OpenAlexClient): An instance of OpenAlexClient to fetch citing works.
        hops (int, optional): Number of hops to expand. Defaults to 1.
        min_citations (int, optional): Minimum citations for citing works. Defaults to 20.
        year_gt (int, optional): Minimum publication year for citing works. Defaults to 1920.
        select (str, optional): Fields to return. Defaults to "id,doi,title,publication_year,cited_by_count,cited_by_api_url".
        delay_between_calls (float, optional): Delay between API calls in seconds. Defaults to 0.0.
    Returns:
        tuple[list,list]: A tuple containing the deduplicated list of all collected works
            and a list of lists for each layer of newly added works.

    """
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
