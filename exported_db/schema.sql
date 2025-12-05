CREATE TABLE public.openalex_vector_spaces (
    oa_id TEXT PRIMARY KEY,
    doi TEXT,
    title TEXT,
    publication_year BIGINT,
    cited_by_count BIGINT,
    abstract TEXT,
    referenced_works JSONB,
    domain TEXT,
    field TEXT,
    topic TEXT,
    layer BIGINT,
    in_decade_references JSONB DEFAULT '[]'::jsonb
);

CREATE TABLE public.per_decade_citation_graph (
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    decade_start BIGINT,
    CONSTRAINT per_decade_citation_graph_pkey PRIMARY KEY (from_id, to_id)
);
