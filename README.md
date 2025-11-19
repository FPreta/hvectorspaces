# hvectorspaces
The `hvectorspaces` repository investigates the historical evolution of vector space research through citation networks analysis.

## Installation
To install the required packages, run the following command in your python environment:

```
pip install .[dev]
```

## Usage
The repository contains three main components:
1. The `hvectorspaces` package for data processing, analysis and visualization;
2. The `tests` folder for unit tests;
3. The `scripts` folder for data acquisition and processing scripts.

The most important module in the `hvectorspaces` package is the `io` module, which provides a client to access Cockroach DB for data retrieval and storage. 
To use the `io` module, you can create an instance of the `CockroachDBClient` class and use its methods to interact with the database. For example, to download the data and generate a map from OpenAlex work ID to the list of IDs of cited works, you can use the following code:

```python
from hvectorspaces.io import CockroachClient

id_to_cited_ids = {}
with CockroachClient() as client:
    citation_map = client.fetch_per_decade_data(1980)
    for oa_id, refs in citation_map:
        id_to_cited_ids[oa_id] = refs
```

The `CockroachDBClient.fetch_per_decade_data` method can also take a list of column names as an optional argument to specify which additional columns to retrieve from the database.

In order to launch a generic SQL query against the Cockroach DB, you can use the `CockroachDBClient.execute_query` method. For example:

```python
from hvectorspaces.io import CockroachClient
with CockroachClient() as client:
    result = client.execute_sql("SELECT * FROM openalex_vector_spaces LIMIT 10;")
    for row in result:
        print(row)
```

Your `.env` file should contain the relevant environment variables to connect to the Cockroach DB instance. These are `CRDB_HOST`, `CRDB_PORT`, `CRDB_USER`, `CRDB_PASSWORD`, `CRDB_DATABASE` and `CRDB_SSLMODE`.

## Table structure
The main table used in this repository is `openalex_vector_spaces`, which contains the following columns:
- `oa_id`: OpenAlex work ID (string)
- `doi`: Digital Object Identifier of the work (string)
- `title`: Title of the work (string)
- `publication_year`: Year of publication (integer)
- `cited_by_count`: Number of times the work has been cited (integer)
- `abstract`: Abstract of the work (string)
- `referenced_works`: List of OpenAlex work IDs cited by the work (array of strings)
- `domain`: Domain of the work (string)
- `field`: Field of study of the work (string)
- `topic`: Topic of the work (string)
- `layer`: Number of hops from the seed for the work in the citation network (int)
- `in_decade_references`: List of OpenAlex work IDs cited by the work in the same decade (array of strings)

## Scripts

The `scripts` folder contains scripts for data acquisition and processing. The main scripts are:

- `sql_upload_oa_data.py`: This script uploads OpenAlex data to the Cockroach DB instance. It reads data from a specified source and populates the `openalex_vector_spaces` table. Currently, it searches for all works that contain the term "vector space" in their title or abstract, were published after 1920 and have more than 20 citations. Starting from these seed works, it performs a breadth-first search in the citation network to collect all works that cite or are cited by the seed works, up to 2 hops away, filtering out those that have less than 20 citations.
- `add_in_decade_references_column.py`: This script adds the `in_decade_references` column to the `openalex_vector_spaces` table. It populates this column with the list of cited works that were published in the same decade as the citing work.
