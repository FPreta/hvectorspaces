import json
import math

import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.patches import Patch

# ----------------------------
# Helper functions
# ----------------------------


def dominant_field(field_dist):
    """Return the field with the highest probability."""
    if not field_dist:
        return None
    return max(field_dist.items(), key=lambda x: x[1])[0]


def main():
    """
    Visualizes the longitudinal evolution of clusters using data from 'clustering_data.json'.

    Input:
        - clustering_data.json: A JSON file containing cluster information for each decade.
          The expected format is:
              {
                  "decade": {
                      "cluster_id": {
                          "elements": int,
                          "field_distribution": {field: probability, ...},
                          "intracluster_links": {target_cluster_id: weight, ...}
                      },
                      ...
                  },
                  ...
              }

    Output:
        - cluster_evolution.svg: An SVG file visualizing the cluster evolution, with nodes colored by dominant field and edges representing cluster links.
    """
    # ----------------------------
    # Load clustering data
    # ----------------------------
    G = nx.DiGraph()

    # Collect field values so we can make a consistent color palette
    all_fields = set()

    for decade, clusters in data.items():
        for cid, info in clusters.items():
            for f in info["field_distribution"]:
                if f is not None:
                    all_fields.add(f)

    all_fields = sorted(all_fields)
    palette = plt.cm.tab20
    field_to_color = {f: palette(i % 20) for i, f in enumerate(all_fields)}

    # Field ranking for ordering nodes vertically
    field_to_rank = {f: i for i, f in enumerate(all_fields)}

    # ----------------------------
    # Build graph
    # ----------------------------

    for decade, clusters in data.items():
        decade = int(decade)
        for cid, info in clusters.items():
            node_id = f"{decade}-{cid}"
            G.add_node(
                node_id,
                decade=decade,
                size=info["elements"],
                field_dist=info["field_distribution"],
            )

            # Add weighted edges to referenced clusters
            for target, weight in info["intracluster_links"].items():
                G.add_edge(node_id, target, weight=weight)

    # ----------------------------
    # Compute layout (ordered by color)
    # ----------------------------

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

    # ----------------------------
    # Compute node attributes (color, size)
    # ----------------------------

    node_colors = []
    node_sizes = []
    all_dominant_fields = set()
    for node in G.nodes():
        fd = G.nodes[node]["field_dist"]
        df = dominant_field(fd)
        if df is not None:
            all_dominant_fields.add(df)
        color = field_to_color.get(df, (0.5, 0.5, 0.5))
        node_colors.append(color)

        node_sizes.append(G.nodes[node]["size"] * 35)

    # ----------------------------
    # Draw figure
    # ----------------------------

    plt.figure(figsize=(22, 12))

    # Draw edges
    edges = list(G.edges())
    edge_widths = [G[u][v]["weight"] * 2500 for u, v in edges]

    nx.draw_networkx_edges(
        G,
        pos,
        edgelist=edges,
        width=edge_widths,
        edge_color="black",
        alpha=0.25,
        arrows=False,
    )

    # Draw nodes
    nx.draw_networkx_nodes(
        G,
        pos,
        node_color=node_colors,
        node_size=[100 * math.log(max(node_size, 1)) for node_size in node_sizes],
        edgecolors="black",
        linewidths=0.5,
    )

    # Node labels (cluster ID only)
    labels = {n: n.split("-")[1] for n in G.nodes()}
    nx.draw_networkx_labels(G, pos, labels, font_size=7)

    # Draw decade separators
    for d in decades:
        plt.axvline(d, color="gray", lw=0.4, linestyle="--")

    plt.xticks(decades)
    plt.xlabel("Decade")
    plt.ylabel("Clusters (sorted by dominant field / color)")
    plt.title("Longitudinal Cluster Evolution (Ordered by Field Color)")
    # ----------------------------
    # Create Legend (Color → Field mapping)
    # ----------------------------

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

    # ----------------------------
    # Save to SVG
    # ----------------------------

    output_path = "cluster_evolution.svg"
    plt.savefig(output_path, format="svg", bbox_inches="tight", dpi=300)

    print(f"SVG saved to: {output_path}")

    plt.show()


if __name__ == "__main__":
    main()
