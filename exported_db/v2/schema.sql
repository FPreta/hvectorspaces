CREATE TABLE public.openalex_vector_spaces (
    oa_id TEXT PRIMARY KEY,
    doi TEXT,
    title TEXT,
    publication_year INT,
    cited_by_count INT,
    abstract TEXT,
    referenced_works JSONB,
    domain TEXT,
    field TEXT,
    subfield TEXT,
    topic TEXT,
    layer INT
);
