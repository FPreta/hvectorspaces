from enum import Enum

import graph_tool.all as gt
import igraph as ig
import leidenalg as la
from infomap import Infomap


class ClusteringMethod(Enum):
    LEIDEN = "leiden"
    INFOMAP = "infomap"
    SBM = "sbm"
    # Other methods can be added here


class CommunityDetector:

    @staticmethod
    def run_leiden_directed(edges: list[tuple[str, str]], weights=None, directed=True):
        # Build igraph directed graph
        g = ig.Graph(directed=directed)

        # Extract node list
        nodes = sorted({u for u, _ in edges} | {v for _, v in edges})
        g.add_vertices(nodes)
        node_index = {n: i for i, n in enumerate(nodes)}

        # Add edges
        ig_edges = [(node_index[u], node_index[v]) for u, v in edges]
        g.add_edges(ig_edges)

        # Add weights if given
        if weights is not None:
            g.es["weight"] = weights

        # Leiden with "modularity" for directed graphs
        partition = la.find_partition(
            g,
            la.ModularityVertexPartition,
            weights=g.es["weight"] if weights is not None else None,
        )

        # Convert result to {node: community}
        community = {nodes[i]: c for i, c in enumerate(partition.membership)}
        return community, partition

    @staticmethod
    def run_infomap_directed(edges, weights=None, directed=True):
        init_string = "--directed --two-level" if directed else "--two-level"

        im = Infomap(init_string)

        for i, (u, v) in enumerate(edges):
            w = weights[i] if weights is not None else 1.0
            im.add_link(u, v, w)

        im.run()

        # node_id → module_id mapping
        communities = {n.node_id: n.module_id for n in im.nodes}
        return communities, im

    @staticmethod
    def run_sbm_directed(edges):
        # Map nodes to contiguous IDs
        nodes = sorted({u for u, v in edges} | {v for u, v in edges})
        idx = {n: i for i, n in enumerate(nodes)}

        # Build directed graph
        g = gt.Graph(directed=True)
        g.add_vertex(len(nodes))

        # Add edges
        g.add_edge_list((idx[u], idx[v]) for u, v in edges)

        # Degree-corrected SBM with automatic model selection
        state = gt.minimize_blockmodel_dl(g, deg_corr=True)

        # Extract block assignments
        blocks = state.get_blocks()
        communities = {nodes[v]: int(blocks[v]) for v in range(len(nodes))}
        return communities, state

    @staticmethod
    def detect(graph: dict[str, list[str]], method: str):
        method = ClusteringMethod(method.lower())
        edges = []
        for u, neighbors in graph.items():
            for v in neighbors:
                edges.append((u, v))
        if method == ClusteringMethod.LEIDEN:
            return CommunityDetector.run_leiden_directed(edges)
        elif method == ClusteringMethod.INFOMAP:
            return CommunityDetector.run_infomap_directed(edges)
        elif method == ClusteringMethod.SBM:
            return CommunityDetector.run_sbm_directed(edges)
        else:
            raise ValueError(f"Unsupported clustering method: {method}")
