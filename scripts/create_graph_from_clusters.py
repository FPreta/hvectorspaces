import argparse
import json

import matplotlib.pyplot as plt
import networkx as nx

from hvectorspaces.utils.distribution_utils import find_all_dominant_fields
from hvectorspaces.visualization.draw import draw_cluster_evolution_svg
from hvectorspaces.visualization.nodes import compute_layout, compute_node_attributes

# ----------------------------
# Parsing command-line arguments
# ----------------------------


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Visualizes the longitudinal evolution of clusters using data from 'clustering_data.json'."
    )
    parser.add_argument(
        "--input_path",
        type=str,
        required=True,
        help="Path to the JSON file containing clustering data.",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="Path to save the output SVG file.",
    )

    return parser.parse_args()


# ----------------------------
# Helper functions
# ----------------------------


def main(input_path: str, output_path: str):
    """
    Visualizes the longitudinal evolution of clusters using data from 'clustering_data.json'.

    Arguments:
        input_path (str): Path to the JSON file containing clustering data.
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

        output_path (str): path to an SVG file visualizing the cluster evolution,
                with nodes colored by dominant field and edges representing cluster links.
    """
    # ----------------------------
    # Load clustering data
    # ----------------------------
    graph = nx.DiGraph()

    # Collect field values so we can make a consistent color palette
    all_fields = set()

    with open(input_path, "r") as f:
        data = json.load(f)

    for decade, clusters in data.items():
        for cid, info in clusters.items():
            for f in info["field_distribution"]:
                if f is not None:
                    all_fields.add(f)

    all_relevant_fields = find_all_dominant_fields(data)

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
            graph.add_node(
                node_id,
                decade=decade,
                size=info["elements"],
                field_dist=info["field_distribution"],
            )

            # Add weighted edges to referenced clusters
            for target, weight in info["intracluster_links"].items():
                graph.add_edge(node_id, target, weight=weight)

    # ----------------------------
    # Compute layout (ordered by color)
    # ----------------------------

    pos = compute_layout(data, field_to_rank)
    # ----------------------------
    # Compute node attributes (color, size)
    # ----------------------------

    node_colors, node_sizes = compute_node_attributes(graph, field_to_color)

    # ----------------------------
    # Draw and save graph
    # --------------------------

    draw_cluster_evolution_svg(
        graph,
        pos,
        sorted(int(d) for d in data.keys()),
        node_colors,
        node_sizes,
        field_to_color,
        all_fields,
        all_relevant_fields,
        output_path,
    )


if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_path, args.output_path)
