import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from hvectorspaces.io.openalex_client import OpenAlexClient


@pytest.mark.integration
def test_openalex_fetch_work_iter():
    """Test fetching multiple works from OpenAlex matching a search query."""
    client = OpenAlexClient()
    search = "vector space"  # Example search term
    filter_str = "has_doi:true,publication_year:>2024,cited_by_count:>50"
    select = "id,doi,title,publication_year"
    works = client.fetch_works_iter(search=search, filter_str=filter_str, select=select)
    works = list(works)
    assert works
    for work in works:
        assert "id" in work
        assert "doi" in work
        assert "title" in work
        assert work["publication_year"] > 2024
        assert "cited_by_count" not in work


@pytest.mark.integration
def test_openalex_fetch_work_by_id():
    """Test fetching works by OpenAlex IDs."""
    client = OpenAlexClient()
    id_list = [
        "https://openalex.org/W2160597895",
        "https://openalex.org/W2159549133",
    ]  # Example OpenAlex work IDs
    works = client.fetch_works_by_ids(id_list, select="id,doi,title")
    assert len(works) == 2
    id_set = {work["id"].split("/")[-1] for work in works}
    assert "W2160597895" in id_set
    assert "W2159549133" in id_set


@pytest.mark.integration
@pytest.mark.asyncio
async def test_openalex_collect_citing_works():
    """Test fetching citing works asynchronously."""
    client = OpenAlexClient()
    id_list = ["W2160597895", "W2159549133"]
    async with aiohttp.ClientSession() as session:
        citing_works = await client.collect_citing_works(
            session=session,
            id_list=id_list,
            min_citations=20,
            year_gt=1990,
            select="id,doi,title,referenced_works",
        )
    assert citing_works
    for work in citing_works:
        assert "id" in work
        assert "doi" in work
        assert "title" in work
        assert "referenced_works" in work
        assert "abstract_inverted_index" not in work
        reference_ids = [ref.split("/")[-1].upper() for ref in work["referenced_works"]]
        assert "W2160597895" in reference_ids or "W2159549133" in reference_ids


# --- Unit tests (no network) ---

@pytest.mark.asyncio
async def test_fetch_citing_works_pagination():
    """_aget is called for each page; stops when next_cursor is None."""
    client = OpenAlexClient(mailto=None)

    page1 = {"results": [{"id": "W1"}], "meta": {"next_cursor": "cursor_abc"}}
    page2 = {"results": [{"id": "W2"}], "meta": {"next_cursor": None}}

    call_count = 0

    async def fake_aget(session, url, params, **kwargs):
        nonlocal call_count
        call_count += 1
        return page1 if call_count == 1 else page2

    client._aget = fake_aget
    session = MagicMock()

    results = await client.fetch_citing_works(session, ["W999"])
    assert [r["id"] for r in results] == ["W1", "W2"]
    assert call_count == 2


@pytest.mark.asyncio
async def test_fetch_citing_works_empty_response():
    """An empty response from _aget stops pagination immediately."""
    client = OpenAlexClient(mailto=None)

    async def fake_aget(session, url, params, **kwargs):
        return {}

    client._aget = fake_aget
    session = MagicMock()

    results = await client.fetch_citing_works(session, ["W999"])
    assert results == []


@pytest.mark.asyncio
async def test_aget_retries_on_timeout():
    """_aget retries up to max_retries times on exception and then returns {}."""
    client = OpenAlexClient(mailto=None)
    attempts = []

    async def flaky_get(*args, **kwargs):
        attempts.append(1)
        raise asyncio.TimeoutError()

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=MagicMock(
        __aenter__=flaky_get,
        __aexit__=AsyncMock(return_value=False),
    ))

    result = await client._aget(mock_session, "http://fake", {}, max_retries=3, backoff=0)
    assert result == {}
    assert len(attempts) == 3
