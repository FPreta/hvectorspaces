import networkx as nx

from hvectorspaces.utils.distribution_utils import dominant_field


def compute_layout(
    data: dict[str, dict], field_to_rank: dict[str, int]
) -> dict[str, tuple[float, float]]:
    """Compute layout positions for clusters over decades.

    Args:
        data (dict[str, dict]): data mapping each decade
            to the corresponding clusters with field
            distributions.
        field_to_rank (dict[str, int]): Mapping from field
            names to their rank for sorting, based on
            importance or desired order.

    Returns:
        dict[str, tuple[float, float]]: Mapping from
            cluster IDs to their (x, y) positions.
    """
    pos = {}
    decades = sorted(int(d) for d in data.keys())
    y_spacing = 1.4

    for decade in decades:
        cluster_ids = list(data[str(decade)].keys())
        cluster_ids = [int(cid) for cid in cluster_ids]

        # Sort clusters by field color group → then by size
        def sort_key(cid):
            info = data[str(decade)][str(cid)]
            df = dominant_field(info["field_distribution"])
            rank = field_to_rank.get(df, len(field_to_rank) + 1)
            return (rank, -info["elements"])

        sorted_cluster_ids = sorted(cluster_ids, key=sort_key)

        # Assign positions
        for i, cid in enumerate(sorted_cluster_ids):
            pos[f"{decade}-{cid}"] = (decade, i * y_spacing)
    return pos


def compute_node_attributes(
    G: nx.DiGraph, field_to_color: dict[str, tuple[float, float, float]]
) -> tuple[list[tuple[float, float, float]], list[float]]:
    """Compute node colors and sizes based on dominant fields.

    Args:
        G (nx.DiGraph): The graph containing cluster nodes.
        field_to_color (dict[str, tuple[float, float, float]]): Mapping from field names to RGB color tuples.
    """
    node_colors = []
    node_sizes = []
    for node in G.nodes():
        fd = G.nodes[node]["field_dist"]
        df = dominant_field(fd)
        color = field_to_color.get(df, (0.5, 0.5, 0.5))
        node_colors.append(color)

        node_sizes.append(G.nodes[node]["size"] * 35)
    return node_colors, node_sizes
