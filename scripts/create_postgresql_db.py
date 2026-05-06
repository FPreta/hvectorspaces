from hvectorspaces.io.pg_client import PostgresClient

DB_VERSION = "v2"

sql_fields = {
    "oa_id": "TEXT",
    "doi": "TEXT",
    "title": "TEXT",
    "publication_year": "INT",
    "cited_by_count": "INT",
    "abstract": "TEXT",
    "referenced_works": "JSONB",
    "domain": "TEXT",
    "field": "TEXT",
    "subfield": "TEXT",
    "topic": "TEXT",
    "layer": "INT",
}


def main():
    base = f"exported_db/{DB_VERSION}"
    with PostgresClient() as client:
        client.generate_table("openalex_vector_spaces", sql_fields, pk="oa_id")
        client.load_csv(
            table_name="openalex_vector_spaces",
            csv_path=f"{base}/openalex_vector_spaces.csv.gz",
            gzip_compressed=True,
        )


if __name__ == "__main__":
    main()
