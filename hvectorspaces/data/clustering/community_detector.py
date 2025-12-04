from collections import defaultdict
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

        # Convert result to {community: nodes}
        communities = defaultdict(list)
        for node, comm_id in zip(nodes, partition.membership):
            communities[comm_id].append(node)
        return communities

    @staticmethod
    def run_infomap_directed(edges, weights=None, directed=True):
        # Map nodes to integer IDs
        nodes = sorted({u for u, v in edges} | {v for u, v in edges})
        idx = {n: i for i, n in enumerate(nodes)}

        im = Infomap("--directed --two-level" if directed else "--two-level")

        for i, (u, v) in enumerate(edges):
            iu = idx[u]
            iv = idx[v]
            w = float(weights[i]) if weights is not None else 1.0
            im.addLink(iu, iv, w)  # MUST be integers

        im.run()

        # communities is a dict mapping module_id to list of node_ids
        communities = defaultdict(list)

        for node in im.nodes:
            communities[node.module_id].append(node.node_id)

        return communities

    @staticmethod
    def run_sbm_directed(edges):
        # Collect nodes
        nodes = sorted({u for u, v in edges} | {v for u, v in edges})
        idx = {n: i for i, n in enumerate(nodes)}

        # Build directed graph
        g = gt.Graph(directed=True)
        g.add_vertex(len(nodes))
        g.add_edge_list((idx[u], idx[v]) for u, v in edges)

        # MDL minimization with degree correction
        state = gt.minimize_blockmodel_dl(
            g, state_args=dict(deg_corr=True)  # ← degree-corrected SBM
        )

        # Extract final block assignment
        blocks = state.get_blocks()

        # communities is a dict mapping block_id to list of node_ids
        communities = defaultdict(list)
        for v in g.vertices():
            block_id = blocks[v]
            node_id = nodes[int(v)]
            communities[block_id].append(node_id)
        return communities

    @staticmethod
    def detect(graph: dict[str, list[str]], method: str) -> tuple[dict, object]:
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
