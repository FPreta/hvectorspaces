import aiohttp
import pytest

from hvectorspaces.io.openalex_client import OpenAlexClient


def test_openalex_fetch_work_iter():
    """Test fetching multiple works from OpenAlex matching a search query."""
    client = OpenAlexClient()
    search = "vector space"  # Example search term
    filter_str = "has_doi:true,publication_year:>2025,cited_by_count:>50"
    select = "id,doi,title"
    works = client.fetch_works_iter(search=search, filter_str=filter_str, select=select)
    assert works
    for work in works:
        assert "id" in work
        assert "doi" in work
        assert "title" in work
        assert "publication_year" not in work  # Not selected


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
