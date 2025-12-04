from collections import defaultdict
from enum import Enum
from typing import Literal

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
    def run_leiden(
        edges: list[tuple[str, str]],
        weights: list[float] | None = None,
        directed: bool = True,
    ) -> dict[int, list[str]]:
        """Leiden method:
        Given a list of nodes, and a list of directed edges (tuples),
        the Leiden algorithm works in multiple phases:
        Phase 1: starting from a community per node, each node is moved to the community
            of one of its neighbors if this increases the overall modularity. The modularity
            rewards communities whose connectivity is higher than expected in a random graph with the
            same degree distribution.
        Phase 2: For each community found in Phase 1, a refinement step is applied.
            Inside each community, we again start from single-node subcommunities
            and repeat the local moving procedure, but restricted to the nodes of that
            community. This may split the original community into multiple
            better-connected subcommunities. The old community is replaced by these
            refined subcommunities.
        Phase 3: the graph is aggregated, where each community found in Phase 2 is represented as a single node.
            The edges are weighted by the sum of the weights of the edges between nodes in the original communities.
            Then we repeat the process of Phase 1 on this aggregated graph.
        The process is repeated until no further modularity improvement is possible after iterating after all three phases.

        Louvain is similar to Leiden, but without phase 2.

        Args:
            edges (list[tuple[str, str]]): List of directed edges (tuples)
            weights (list[float], optional): List of edge weights. Defaults to None.
            directed (bool, optional): Whether the graph is directed. Defaults to True.

        Returns:
            dict[int, list[str]]: Mapping of community ID to list of node IDs
        """
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
    def run_infomap(
        edges: list[tuple[str, str]],
        weights: list[float] | None = None,
        directed: bool = True,
    ):
        """Infomap method:
        Given a list of nodes, and a list of directed edges (tuples),
        the Infomap algorithm works in multiple phases:
        Phase 0: define a map equation to minimize the expected description length of a random walk. In practice,
            for any partition of the network into communities, the map equation calculates the entropy
            of the information required to describe a random walker exiting a community plus the entropy
            of the information required to describe its position within a community. It's
            L = qH(Q) + ∑ p_i H(P_i), where q is the probability of exiting communities,
            H(Q) is the entropy of the codebook for inter-community movements,
            p_i is the probability of being in community i, and H(P_i) is the entropy of the codebook for intra-community movements.
        Phase 1: starting from a community per node, each node is moved to the community
            of one of its neighbors if this decreases the value of the map equation. This is repeated
            until all nodes are considered and no further improvement is possible.
        Phase 2: the graph is aggregated, where each community found in Phase 1 is represented as a single node.
            The edges are weighted by the sum of the weights of the edges between nodes in the original communities.
            Then we repeat the process of Phase 1 on this aggregated graph.
        The process is repeated until no further map-equation improvement is possible after iterating after phases 1 and 2.

        Infomap is similar to Louvain, but with a different objective function.

        Args:
            edges (list[tuple[str, str]]): List of directed edges (tuples)
            weights (list[float], optional): List of edge weights. Defaults to None.
            directed (bool, optional): Whether the graph is directed. Defaults to True.

        Returns:
            dict[int, list[str]]: Mapping of community ID to list of node IDs
        """
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
            communities[node.module_id].append(nodes[node.node_id])

        return communities

    @staticmethod
    def run_sbm(
        edges: list[tuple[str, str]], directed: bool = True
    ) -> dict[int, list[str]]:
        """Stochastic Block Model (SBM) method:
        Given a list of nodes, and a list of directed edges (tuples),
        the SBM algorithm works by fitting a probabilistic model to the observed network structure.
        The aim is NOT to detect cliques, but rather to recreate at a higher level the type of structures
        observed in the network. For example, if there are five pairs of nodes that are only connected to
        each other, the SBM will group them into two blocks with only one link between them.
        It works in multiple phases:
        Phase 0: define an initial state in which each node is assigned to a block (community). Define
            an objective function which penalizes model complexity.
        Phase 1: iteratively reassign nodes to different blocks to minimize the description length
            of the model given the block structures.
        Phase 2: optionally, the graph is aggregated, where each block found in Phase 1 is represented as a single node.
            The edges are weighted by the sum of the weights of the edges between nodes in the original blocks.
            Then we repeat the process of Phase 1 on this aggregated graph.
        The process is repeated until no further description length improvement is possible after iterating after phases 1 and 2.

        Args:
            edges (list[tuple[str, str]]): List of directed edges (tuples)
            directed (bool, optional): Whether the graph is directed. Defaults to True.

        Returns:
            dict[int, list[str]]: Mapping of community ID to list of node IDs

        """
        # Collect nodes
        nodes = sorted({u for u, v in edges} | {v for u, v in edges})
        idx = {n: i for i, n in enumerate(nodes)}

        # Build directed graph
        g = gt.Graph(directed=directed)
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
    def detect(
        graph: dict[str, list[str]],
        method: Literal["leiden", "infomap", "sbm"],
        directed=True,
    ) -> dict[int, list[str]]:
        """Detect communities in a graph using the specified method.

        Args:
            graph (dict[str, list[str]]): Input graph as an adjacency list
            method (literal): Clustering method to use. One of ('leiden', 'infomap', 'sbm')
            directed (bool, optional): Whether the graph is directed. Defaults to True.

        Raises:
            ValueError: If an unsupported clustering method is provided.

        Returns:
            dict[int, list[str]]: Mapping of community ID to list of node IDs
        """
        method = ClusteringMethod(method.lower())
        edges = []
        for u, neighbors in graph.items():
            for v in neighbors:
                edges.append((u, v))
        if method == ClusteringMethod.LEIDEN:
            return CommunityDetector.run_leiden(edges, directed=directed)
        elif method == ClusteringMethod.INFOMAP:
            return CommunityDetector.run_infomap(edges, directed=directed)
        elif method == ClusteringMethod.SBM:
            return CommunityDetector.run_sbm(edges, directed=directed)
        else:
            raise ValueError(f"Unsupported clustering method: {method}")
