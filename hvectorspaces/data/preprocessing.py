def normalize_title(title):
    if not title:
        return None
    return " ".join(title.strip().lower().split())


def oa_id(work):
    if isinstance(work, dict):
        wid = work.get("id", "")
    else:
        wid = str(work)
    return wid.split("/")[-1] if "/" in wid else wid


def normalize_abstract(work):
    """
    Reconstructs the abstract text from OpenAlex's abstract_inverted_index field.

    Args:
        work (dict): A work record from OpenAlex API.

    Returns:
        str or None: The normalized abstract (plain text) or None if missing.
    """
    inv = work.get("abstract_inverted_index")
    if not inv or not isinstance(inv, dict):
        return None

    # Get all position indices
    all_positions = [pos for positions in inv.values() for pos in positions]
    if not all_positions:
        return None
    max_index = max(all_positions)
    # Initialize empty list
    abstract_words = [""] * (max_index + 1)

    # Fill each position with the corresponding word
    for word, positions in inv.items():
        for pos in positions:
            abstract_words[pos] = word

    # Join into a string and normalize spaces
    abstract = " ".join(abstract_words).strip()
    return abstract or None


def normalize_primary_topic(work):
    if work.get("primary_topic") is None:
        work["domain"] = None
        work["field"] = None
        work["topic"] = None
        return work
    work["domain"] = (
        work.get("primary_topic", {}).get("domain", {}).get("display_name", None)
    )
    work["field"] = (
        work.get("primary_topic", {}).get("field", {}).get("display_name", None)
    )
    work["topic"] = work.get("primary_topic", {}).get("display_name", None)
    return work


def normalize_work_fields_inplace(w):
    w["oa_id"] = oa_id(w)
    w["doi"] = (w.get("doi") or "").strip().lower() or None
    w["title"] = normalize_title(w.get("title"))
    w["abstract"] = normalize_abstract(w)
    w["referenced_works"] = [oa_id(ref) for ref in w.get("referenced_works", []) if ref]
    w = normalize_primary_topic(w)
    # Removes unused fields
    w.pop("abstract_inverted_index", None)
    w.pop("primary_topic", None)
