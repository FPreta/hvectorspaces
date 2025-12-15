import json
import logging
from collections import Counter, defaultdict

from tqdm import tqdm

from hvectorspaces.data.clustering.community_detector import CommunityDetector
from hvectorspaces.io import PostgresClient

DECADE_START = 1950
CLUSTER_SIZE_CUTOFF = 5
CLUSTERING_METHOD = "leiden"


def parse_arguments():
    import argparse

    parser = argparse.ArgumentParser(
        description="Clusters scholarly works by decade and analyzes cluster metadata distributions."
    )
    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="Path to the output JSON file where clustering results will be saved.",
    )
    parser.add_argument(
        "--decade_start",
        type=int,
        default=DECADE_START,
        help="The starting year for the first decade to cluster (default: 1950).",
    )
    parser.add_argument(
        "--clustering_method",
        type=str,
        default=CLUSTERING_METHOD,
        help="The community detection algorithm to use (default: 'leiden').",
    )
    parser.add_argument(
        "--cluster_size_cutoff",
        type=int,
        default=CLUSTER_SIZE_CUTOFF,
        help="Minimum number of elements required for a cluster to be included (default: 5).",
    )

    return parser.parse_args()


def normalize_distribution(counter: Counter) -> dict[str, float]:
    total = sum(counter.values()) - counter.get(None, 0)
    return (
        {k: v / total for k, v in counter.items() if k is not None} if total > 0 else {}
    )


def create_cluster_by_decade(
    output_path,
    decade_start,
    clustering_method,
    cluster_size_cutoff,
):
    """
    Clusters scholarly works by decade using a specified community detection algorithm,
    analyzes cluster metadata distributions, and outputs the results to a JSON file.

    Parameters
    ----------
    output_path : str
        Path to the output JSON file where clustering results will be saved.
    decade_start : int, optional
        The starting year for the first decade to cluster (default: 1950).
    clustering_method : str, optional
        The community detection algorithm to use (default: "leiden").
    cluster_size_cutoff : int, optional
        Minimum number of elements required for a cluster to be included (default: 5).

    Returns
    -------
    None
        The function does not return a value; results are written to the specified JSON file.

    Output JSON Structure
    --------------------
    The output file is a dictionary mapping each decade (int) to its clusters.
    Each cluster is represented as a dictionary with the following keys:
        - "elements": int, number of items in the cluster
        - "field_distribution": dict[str, float], normalized distribution of fields
        - "domain_distribution": dict[str, float], normalized distribution of domains
        - "topic_distribution": dict[str, float], normalized distribution of topics
        - "intracluster_links": int, number of links within the cluster (added after calculation)
        - "citation_count": dict[str, int], citation counts for referenced works (added after calculation)
    Example:
        {
            "1950": {
                "0": {
                    "elements": 42,
                    "field_distribution": {"Physics": 0.5, "Chemistry": 0.5},
                    "domain_distribution": {"Science": 1.0},
                    "topic_distribution": {"Quantum": 0.7, "Thermodynamics": 0.3},
                    "intracluster_links": 123,
                    "citation_count": {"ref1": 10, "ref2": 5}
                },
                ...
            },
            ...
        }
    """
    client = PostgresClient()
    clusterer = CommunityDetector()
    decade_to_clusters = {}
    node_to_cluster = {}
    for start in tqdm(range(decade_start, 2025, 10)):
        decade_data = client.fetch_per_decade_data(
            start, ["topic", "field", "domain", "referenced_works"]
        )
        metadata = {}
        for oa_id, references, topic, field, domain, full_references in decade_data:
            metadata[oa_id] = {
                "references": references,
                "topic": topic,
                "field": field,
                "domain": domain,
                "full_references": full_references,
            }
        graph = {oa_id: metadata[oa_id]["references"] for oa_id in metadata}
        # Finds clusters
        clusters = clusterer.detect(graph, method=clustering_method)

        # Take the top 10 clusters
        clusters = dict(
            sorted(clusters.items(), key=lambda item: len(item[1]), reverse=True)[:15]
        )
        # Filter clusters by cutoff
        clusters = {k: v for k, v in clusters.items() if len(v) > cluster_size_cutoff}

        logging.info(f"Found {len(clusters)} clusters.")
        # Map nodes to cluster ids
        for cluster_id, cluster_members in clusters.items():
            for member in cluster_members:
                node_to_cluster[member] = f"{start}-{cluster_id}"
        # Find distributions of topic, field, domain
        topic_distribution = defaultdict(Counter)
        field_distribution = defaultdict(Counter)
        domain_distribution = defaultdict(Counter)
        citation_count_by_cluster = defaultdict(Counter)
        for cluster_id, cluster_members in clusters.items():
            for oa_id in cluster_members:
                topic_distribution[cluster_id][metadata[oa_id]["topic"]] += 1
                field_distribution[cluster_id][metadata[oa_id]["field"]] += 1
                domain_distribution[cluster_id][metadata[oa_id]["domain"]] += 1
                for ref in metadata[oa_id]["full_references"]:
                    citation_count_by_cluster[f"{start}-{cluster_id}"][ref] += 1

        # Normalize distributions
        topic_distribution = {
            cluster_id: normalize_distribution(distribution)
            for cluster_id, distribution in topic_distribution.items()
        }
        field_distribution = {
            cluster_id: normalize_distribution(distribution)
            for cluster_id, distribution in field_distribution.items()
        }
        domain_distribution = {
            cluster_id: normalize_distribution(distribution)
            for cluster_id, distribution in domain_distribution.items()
        }
        # Save decade data
        decade_to_clusters[start] = {
            cluster_id: {
                "elements": len(cluster_members),
                "field_distribution": field_distribution[cluster_id],
                "domain_distribution": domain_distribution[cluster_id],
                "topic_distribution": topic_distribution[cluster_id],
            }
            for cluster_id, cluster_members in clusters.items()
        }

    # Calculate intracluster links
    for start in decade_to_clusters:
        clusters = decade_to_clusters[start]
        for cluster_id in clusters:
            citation_counter = citation_count_by_cluster[f"{start}-{cluster_id}"]
            new_citation_counter = Counter()
            for ref, count in citation_counter.items():
                if ref in node_to_cluster:
                    new_citation_counter[node_to_cluster[ref]] += count
            # Normalize by dividing each count by the product of the number of elements in the clusters
            for ref_cluster_id in new_citation_counter:
                cl_start, cl_k = ref_cluster_id.split("-")
                size_product = (
                    clusters[cluster_id]["elements"]
                    * decade_to_clusters[int(cl_start)][int(cl_k)]["elements"]
                )
                if size_product > 0:
                    new_citation_counter[ref_cluster_id] /= size_product
            decade_to_clusters[start][cluster_id]["intracluster_links"] = dict(
                new_citation_counter
            )
            total_citations = sum(citation_counter.values())
            intracluster_citations = sum(
                count
                for ref, count in citation_counter.items()
                if ref in node_to_cluster and node_to_cluster[ref] == cluster_id
            )
            clusters[cluster_id]["intracluster_link_ratio"] = (
                intracluster_citations / total_citations if total_citations > 0 else 0
            )

    with open(output_path, "wt") as fout:
        json.dump(decade_to_clusters, fout, indent=2)


if __name__ == "__main__":
    args = parse_arguments()
    create_cluster_by_decade(
        args.output_path,
        args.decade_start,
        args.clustering_method,
        args.cluster_size_cutoff,
    )
