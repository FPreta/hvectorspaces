from hvectorspaces.data.clustering.community_detector import (
    CommunityDetector,
)


def test_leiden_community_detection():
    graph = {
        "A": ["B", "C"],
        "B": ["A", "C"],
        "C": [
            "A",
            "B",
        ],
        "D": ["E"],
        "E": ["D", "F"],
        "F": ["E"],
    }
    communities = CommunityDetector.detect(graph, method="leiden")
    assert isinstance(communities, dict)
    all_nodes = set()
    for val in communities.values():
        assert (
            set(val).intersection(all_nodes) == set()
        )  # No overlapping nodes between communities
        all_nodes = all_nodes.union(set(val))

    assert len(all_nodes) == len(graph.keys())  # All nodes pertain to a community
    assert len(communities) == 2  # Two communities expected


def test_infomap_community_detection():
    graph = {
        "A": ["B", "C"],
        "B": ["A", "C"],
        "C": [
            "A",
            "B",
        ],
        "D": ["E"],
        "E": ["D", "F"],
        "F": ["E"],
    }
    communities = CommunityDetector.detect(graph, method="infomap")
    assert isinstance(communities, dict)
    all_nodes = set()
    for val in communities.values():
        assert (
            set(val).intersection(all_nodes) == set()
        )  # No overlapping nodes between communities
        all_nodes = all_nodes.union(set(val))

    assert len(all_nodes) == len(graph.keys())  # All nodes pertain to a community
    assert len(communities) == 2  # Two communities expected


def test_sbm_community_detection():
    graph = {
        "A": ["B", "C"],
        "B": ["A", "C"],
        "C": [
            "A",
            "B",
        ],
        "D": ["E"],
        "E": ["D", "F"],
        "F": ["E"],
    }
    communities = CommunityDetector.detect(graph, method="sbm")
    assert isinstance(communities, dict)
    all_nodes = set()
    for val in communities.values():
        assert (
            set(val).intersection(all_nodes) == set()
        )  # No overlapping nodes between communities
        all_nodes = all_nodes.union(set(val))

    assert len(all_nodes) == len(graph.keys())  # All nodes pertain to a community
    assert len(communities) == 1  # One community expected (small graph)
