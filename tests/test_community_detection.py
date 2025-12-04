import pytest

from hvectorspaces.data.clustering.community_detector import (
    CommunityDetector,
)


@pytest.fixture
def sample_graph():
    """Two disconnected clusters: ABC and DEF."""
    return {
        "A": ["B", "C"],
        "B": ["A", "C"],
        "C": ["A", "B"],
        "D": ["E"],
        "E": ["D", "F"],
        "F": ["E"],
    }


@pytest.mark.parametrize(
    "method,expected_communities",
    [
        ("leiden", 2),
        ("infomap", 2),
        # SBM uses statistical inference and tends to favor larger,
        # more statistically significant communities; it may merge
        # smaller clusters that don't have enough edges for distinct blocks.
        ("sbm", 1),
    ],
)
def test_community_detection(sample_graph, method, expected_communities):
    """Test community detection with different methods."""
    communities = CommunityDetector.detect(sample_graph, method=method)

    assert isinstance(communities, dict)

    # Verify no overlapping nodes between communities
    all_nodes = set()
    for val in communities.values():
        assert set(val).intersection(all_nodes) == set()
        all_nodes = all_nodes.union(set(val))

    # All nodes should belong to a community
    assert len(all_nodes) == len(sample_graph.keys())
    assert len(communities) == expected_communities
