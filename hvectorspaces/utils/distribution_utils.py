def dominant_field(field_dist: dict[str, float]) -> str | None:
    """Return the field with the highest probability."""
    if not field_dist:
        return None
    return max(field_dist.items(), key=lambda x: x[1])[0]


def find_all_dominant_fields(data: dict[str, dict]) -> set[str]:
    """Find all dominant fields in the graph nodes.

    Args:
        data (dict[str, dict]): Data mapping each decade
            to the corresponding clusters with field
            distributions.

    Returns:
        set[str]: A set of all dominant fields present in the graph.
    """
    all_dominant_fields = set()
    for _, clusters in data.items():
        for _, info in clusters.items():
            df = dominant_field(info["field_distribution"])
            if df is not None:
                all_dominant_fields.add(df)
    return all_dominant_fields
