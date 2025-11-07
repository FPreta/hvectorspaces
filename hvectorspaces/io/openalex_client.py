import asyncio
import random
import requests
import time
from tqdm import tqdm
from typing import List, Dict

from hvectorspaces.config.settings import OPENALEX_BASE, CONTACT_EMAIL
from hvectorspaces.data.preprocessing import oa_id
from hvectorspaces.utils.iter_utils import chunked


class OpenAlexClient:
    """High-level async client for OpenAlex API."""

    def __init__(self, mailto: str = CONTACT_EMAIL, concurrency: int = 30):
        self.base = OPENALEX_BASE
        self.mailto = mailto
        self.sem = asyncio.Semaphore(concurrency)

    def _get(self, url, params, max_retries=5, backoff=1.5, timeout=30):
        if self.mailto:
            params["mailto"] = self.mailto
        for attempt in range(max_retries):
            try:
                r = requests.get(url, params=params, timeout=timeout)
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", "2")))
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException:
                if attempt == max_retries - 1:
                    raise
                time.sleep((backoff**attempt) + 0.5)
        raise RuntimeError("Unexpected request loop exit")

    async def _aget(self, session, url, params, max_retries=5, backoff=1.5):
        if self.mailto:
            params["mailto"] = self.mailto
        async with self.sem:
            for attempt in range(max_retries):
                try:
                    async with session.get(url, params=params) as r:
                        await asyncio.sleep(random.uniform(0.05, 0.15))
                        if r.status == 429:
                            await asyncio.sleep(int(r.headers.get("Retry-After", "2")))
                            continue
                        if r.status == 403:
                            await asyncio.sleep(5)
                            continue
                        r.raise_for_status()
                        return await r.json()
                except Exception:
                    await asyncio.sleep(backoff**attempt)
        return {}

    async def fetch_citing_works(
        self, session, work_ids, min_citations=0, year_gt=None, select=None
    ) -> List[Dict]:
        cursor = "*"
        results = []
        id_string = "|".join([oa_id(wid) for wid in work_ids])
        filter_str = f"cites:{id_string}"
        if min_citations:
            filter_str += f",cited_by_count:>{min_citations}"
        if year_gt:
            filter_str += f",publication_year:>{year_gt}"

        while cursor:
            params = {
                "filter": filter_str,
                "cursor": cursor,
                "per-page": 200,
            }
            if select:
                params["select"] = select
            data = await self._aget(session, f"{OPENALEX_BASE}/works", params)
            if not data or not data.get("results"):
                break
            results.extend(data.get("results", []))
            cursor = data.get("meta", {}).get("next_cursor")
        return results

    async def collect_citing_works(
        self,
        session,
        id_list,
        min_citations=0,
        year_gt=None,
        select="id,doi,title,cited_by_count",
    ) -> List[Dict]:
        """Collect citing works (with metadata) for a batch of works."""
        tasks = [
            self.fetch_citing_works(session, batch, min_citations, year_gt, select)
            for batch in chunked(id_list, 100)
        ]
        citing_works = []
        for coro in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Collecting citing works",
            unit="work",
        ):
            works = await coro
            citing_works.extend(works)
        return citing_works

    def fetch_works_by_ids(
        self, id_list, batch_size=100, sleep=0.2, select=None
    ) -> List[Dict]:
        out = []
        ids = [i.split("/")[-1] for i in id_list]
        for chunk in chunked(ids, batch_size):
            params = {"filter": "ids.openalex:" + "|".join(chunk), "per-page": batch_size}
            print(params)
            if select:
                params["select"] = select
            data = self._get(f"{OPENALEX_BASE}/works", params)
            for w in data.get("results", []):
                out.append(w)
            time.sleep(sleep)
        return out

    def fetch_works_iter(self, search=None, filter_str=None, select=None, per_page=200):
        params = {"per-page": per_page, "cursor": "*"}
        if search:
            params["search"] = search
        if filter_str:
            params["filter"] = filter_str
        if select:
            params["select"] = select

        while True:
            data = self._get(f"{OPENALEX_BASE}/works", params=params)
            for w in data.get("results", []):
                yield w
            cursor = data.get("meta", {}).get("next_cursor")
            if not cursor:
                break
            params["cursor"] = cursor
