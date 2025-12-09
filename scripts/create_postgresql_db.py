from hvectorspaces.io.psgl_client import PostgresClient


def main():
    with PostgresClient() as client:

        client.load_csv(
            table_name="openalex_vector_spaces",
            csv_path="exported_db/openalex_vector_spaces.csv.gz",
            gzip_compressed=True,
        )

        client.load_csv(
            table_name="per_decade_citation_graph",
            csv_path="exported_db/per_decade_citation_graph.csv.gz",
            gzip_compressed=True,
        )


if __name__ == "__main__":
    main()
