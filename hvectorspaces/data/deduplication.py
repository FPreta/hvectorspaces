from hvectorspaces.data.preprocessing import normalize_work_fields_inplace


class Deduper:
    def __init__(self):
        self.by_doi, self.by_oaid, self.by_title = set(), set(), set()
        self.kept = []

    def add(self, w):
        normalize_work_fields_inplace(w)
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
