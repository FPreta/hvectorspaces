import asyncio

from hvectorspaces.data.base_graph import build_seed, expand_batched
from hvectorspaces.io.openalex_client import OpenAlexClient


def test_build_seed():
    """Test building a seed set of works."""
    client = OpenAlexClient()
    seed_works = build_seed(
        openalex_client=client,
        search_term="vector space",
        min_citations=50,
        min_year_exclusive=2020,
        select="id,doi,title,publication_year,abstract_inverted_index,primary_topic,referenced_works,cited_by_count",
    )
    assert seed_works
    for work in seed_works:
        assert "id" in work
        assert "doi" in work
        assert "title" in work
        assert work["cited_by_count"] > 50
        assert work["publication_year"] > 2020
        assert "abstract_inverted_index" not in work
        assert "abstract" in work
        assert "primary_topic" not in work
        assert "referenced_works" in work
        assert "field" in work
        assert "topic" in work
        assert "domain" in work


def test_expand_batched():
    """Test expanding the seed set with citing works."""
    client = OpenAlexClient()
    seed_works = build_seed(
        openalex_client=client,
        search_term="vector space",
        min_citations=100,
        min_year_exclusive=2020,
        select="id,doi,title,publication_year,abstract_inverted_index,primary_topic,referenced_works,cited_by_count",
    )
    seed_ids = set(w["oa_id"] for w in seed_works)

    expanded_works, layers = asyncio.run(
        expand_batched(
            seed_works=seed_works,
            oa_client=client,
            hops=1,
            min_citations=50,
            year_gt=2020,
            select="id,doi,title,publication_year,abstract_inverted_index,primary_topic,referenced_works,cited_by_count",
            delay_between_calls=0.0,
        )
    )

    assert expanded_works
    assert layers
    assert len(expanded_works) == len(seed_works) + sum(len(layer) for layer in layers)
    for work in expanded_works:
        assert "id" in work
        assert "doi" in work
        assert "title" in work
        assert work["publication_year"] > 2020
        assert work["cited_by_count"] >= 50
        assert "abstract" in work
        assert "abstract_inverted_index" not in work
        assert "primary_topic" not in work
        assert (
            set(work["referenced_works"]).intersection(seed_ids)
            or work["oa_id"] in seed_ids
        )
        assert "field" in work
        assert "topic" in work
        assert "domain" in work


def test_expand_batched_2_hops():
    """Test expanding the seed set with citing works over two hops."""
    client = OpenAlexClient()

    seed_works = build_seed(
        openalex_client=client,
        search_term="vector space",
        min_citations=100,
        min_year_exclusive=2020,
        select="id,doi,title,publication_year,abstract_inverted_index,primary_topic,referenced_works,cited_by_count",
    )

    expanded_works, layers = asyncio.run(
        expand_batched(
            seed_works=seed_works,
            oa_client=client,
            hops=2,
            min_citations=50,
            year_gt=2020,
            select="id,doi,title,publication_year,abstract_inverted_index,primary_topic,referenced_works,cited_by_count",
            delay_between_calls=0.0,
        )
    )
    curr_hop_ids = set(w["oa_id"] for w in seed_works)
    assert expanded_works
    assert layers
    assert len(expanded_works) == len(seed_works) + sum(len(layer) for layer in layers)
    for layer in layers:
        for work in layer:
            assert "id" in work
            assert "doi" in work
            assert "title" in work
            assert work["publication_year"] > 2020
            assert work["cited_by_count"] >= 50
            assert "abstract" in work
            assert "abstract_inverted_index" not in work
            assert "primary_topic" not in work
            assert (
                set(work["referenced_works"]).intersection(curr_hop_ids)
                or work["oa_id"] in curr_hop_ids
            )
            assert "field" in work
            assert "topic" in work
            assert "domain" in work
        curr_hop_ids = {w["oa_id"] for w in layer}
