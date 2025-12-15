import math

import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.patches import Patch


# TODO: deal with constants
def create_legend_handles(
    field_to_color: dict[str, tuple[float, float, float]],
    all_fields: list[str],
    all_dominant_fields: set[str],
) -> None:
    legend_handles = [
        Patch(facecolor=field_to_color[f], edgecolor="black", label=f)
        for f in all_fields
        if f in all_dominant_fields
    ]
    plt.legend(
        handles=legend_handles,
        title="Field → Color Mapping",
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=True,
    )
    plt.tight_layout()


def draw_cluster_evolution_svg(
    graph: nx.DiGraph,
    pos: dict[str, tuple[float, float]],
    decades: list[int],
    node_colors: list[tuple[float, float, float]],
    node_sizes: list[float],
    field_to_color: dict[str, tuple[float, float, float]],
    all_fields: list[str],
    all_dominant_fields: set[str],
    output_path: str,
) -> None:
    plt.figure(figsize=(22, 12))

    # Draw edges
    edges = list(graph.edges())
    edge_widths = [100 * graph[u][v]["weight"] for u, v in edges]

    nx.draw_networkx_edges(
        graph,
        pos,
        edgelist=edges,
        width=edge_widths,
        edge_color="black",
        alpha=0.25,
        arrows=False,
    )

    # Draw nodes
    nx.draw_networkx_nodes(
        graph,
        pos,
        node_color=node_colors,
        node_size=[100 * math.log(max(node_size, 1)) for node_size in node_sizes],
        edgecolors="black",
        linewidths=0.5,
    )

    # Node labels (cluster ID only)
    labels = {n: n.split("-")[1] for n in graph.nodes()}
    nx.draw_networkx_labels(graph, pos, labels, font_size=7)

    # Draw decade separators
    for d in decades:
        plt.axvline(d, color="gray", lw=0.4, linestyle="--")

    plt.xticks(decades)
    plt.xlabel("Decade")
    plt.ylabel("Clusters (sorted by dominant field / color)")
    plt.title("Longitudinal Cluster Evolution (Ordered by Field Color)")

    create_legend_handles(field_to_color, all_fields, all_dominant_fields)
    # ----------------------------
    # Save to SVG
    # ----------------------------

    plt.savefig(output_path, format="svg", bbox_inches="tight", dpi=300)

    print(f"SVG saved to: {output_path}")

    plt.show()
