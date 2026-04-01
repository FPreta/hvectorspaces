import json

import networkx as nx


def load_network_bins(path: str) -> dict[str, nx.DiGraph]:
    """
    Load a networks JSON file produced by create_clusters.py.

    Returns a dict mapping bin id (e.g. "1920", "1980") to its nx.DiGraph.
    Each node carries 'topic' (str) and 'cluster' (int, -1 = unassigned).
    """
    with open(path) as f:
        data = json.load(f)

    graphs = {}
    for bin_id, bin_data in data.items():
        G = nx.DiGraph()
        for node in bin_data["nodes"]:
            G.add_node(node["id"], topic=node["topic"], cluster=node["cluster"])
        G.add_edges_from(bin_data["edges"])
        graphs[bin_id] = G

    return graphs
