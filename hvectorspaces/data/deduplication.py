from hvectorspaces.data.preprocessing import return_fields_normalize


class Deduper:
    """
    Deduper is a utility class for deduplicating works (e.g., publications) based on
    three unique identifiers: DOI, OpenAlex ID, and title. When a work is added,
    it is checked against previously seen DOIs, OpenAlex IDs, and titles. If any
    of these identifiers have already been seen, the work is considered a duplicate
    and is not added.
    """
    def __init__(self):
        self.by_doi, self.by_oaid, self.by_title = set(), set(), set()
        self.kept = []

    def add(self, w):
        """
        Attempts to add a work to the deduper.

        Parameters
        ----------
        w : dict
            A dictionary representing a work, expected to have keys "doi", "oa_id", and "title".

        Returns
        -------
        bool
            True if the work was not a duplicate and was added; False if it was a duplicate.
        """
        return_fields_normalize(w)
        d, oid, tn = w["doi"], w["oa_id"], w["title"]
        if d and d in self.by_doi:
            return False
        if oid and oid in self.by_oaid:
            return False
        if tn and tn in self.by_title:
            return False
        if d:
            self.by_doi.add(d)
        if oid:
            self.by_oaid.add(oid)
        if tn:
            self.by_title.add(tn)
        self.kept.append(w)
        return True
