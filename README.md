# hvectorspaces
The `hvectorspaces` repository investigates the historical evolution of vector space research through citation networks analysis.

## Installation
To install the required packages, run the following command in your python environment (Python 3.10+ is recommended):

```
pip install .[dev]
```

## PostgreSQL Setup (skip if you already have a local PostgreSQL DB)
This document explains how to set up a **local PostgreSQL database** that mirrors the data we previously hosted on **CockroachDB**.

Each dataset version has its own PostgreSQL database (e.g. `hvectorspaces_v1`, `hvectorspaces_v2`), with the same table names across versions. The active version is selected via the `PG_DATABASE` env var.

All data is stored locally in PostgreSQL and loaded from compressed CSV files that are **versioned in this repo** using Git LFS.

---

### 1. Where the data lives in this repo

Data is organised in versioned subdirectories:

```text
exported_db/
├── v1/
│   ├── schema.sql
│   ├── openalex_vector_spaces.csv.gz
│   └── per_decade_citation_graph.csv.gz
└── v2/
    ├── schema.sql
    └── openalex_vector_spaces.csv.gz
```

The exported database files inside the `exported_db/` directory are large (.csv.gz).
To avoid bloating the Git repository and to make downloads efficient, we store them using Git LFS (Large File Storage).

#### 1.1 Install Git LFS

Download and install Git LFS by following the instructions at https://git-lfs.github.com/ or run:
```bash
curl -s https://packagecloud.io/github/git-lfs/install.sh | sudo bash
```

Enable Git LFS:

```bash
git lfs install
```

#### 1.2. Retrieve the data with Git LFS

When you clone the repository for the first time, Git LFS will automatically download the large files.

After cloning:
```bash
git pull
```

Git LFS automatically downloads the actual CSV files into `exported_db/`.

Check with:

```bash
ls -lh exported_db/
```

You should see large file sizes; not tiny pointer files.

### 2. Requirements

You’ll need:

PostgreSQL 14+

On macOS, we use Postgres.app:

Download and install from the Postgres.app website.

Open Postgres.app and make sure the server is running.

(Optional but recommended) Add the command-line tools to your PATH via Postgres.app’s preferences or by adding the following line to your shell profile:

```bash
export PATH="/Applications/Postgres.app/Contents/Versions/latest/bin:$PATH"
```
For persistent PATH changes, add the above line to your `~/.bash_profile`, `~/.zshrc`, or equivalent file depending on your shell.

### 3. Create the local PostgreSQL database

Create a database for each version you want to use, e.g. `hvectorspaces_v1`, `hvectorspaces_v2`.

From a terminal:

```bash
createdb hvectorspaces_v1
createdb hvectorspaces_v2
```

Or from psql:

```sql
CREATE DATABASE hvectorspaces_v1;
CREATE DATABASE hvectorspaces_v2;
```

### 4. Configure environment variables

The Python clients expect standard PostgreSQL env vars. Create a .env file in the project root or modify the existing one with the following variables:

```env

PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=hvectorspaces_v2  # or hvectorspaces_v1
PG_USER=<your_mac_username>
PG_PASSWORD=
```

Notes:

`PG_USER` should normally be your macOS username (the output of the terminal command `whoami`).

For Postgres.app defaults, no password is required for local connections, so you can leave `PG_PASSWORD` empty.

From the project root, you can load the environment variables from the .env file by running:

```bash
source .env
```

### 5. Create the tables

In order to create the tables, run the following command from the terminal:

```bash
psql -d hvectorspaces_v2 -f exported_db/v2/schema.sql
```

You can verify the tables exist by running

```bash
psql -d hvectorspaces_v2 -c "\d+ openalex_vector_spaces"
```

### 6. Load the data from the compressed CSVs

We have a Python script that uses the local PostgreSQL client (PostgresClient) to load the .csv.gz files via COPY.

From the project root:

```bash
python -m scripts.create_postgresql_db
```

This script should:

Connect to PostgreSQL using the env vars in .env.

Load (for v2):

`exported_db/v2/openalex_vector_spaces.csv.gz` → `openalex_vector_spaces`

The active version is controlled by `DB_VERSION` at the top of the script and `PG_DATABASE` in your `.env`.

If everything works, you should see log output indicating that the rows were loaded.

## Usage
The repository contains three main components:
1. The `hvectorspaces` package for data processing, analysis and visualization;
2. The `tests` folder for unit tests;
3. The `scripts` folder for data acquisition and processing scripts.

The most important module in the `hvectorspaces` package is the `io` module, which provides a client to access PostGreSQL DB for data retrieval and storage. 
You can create an instance of the `PostgresClient` class and use its methods to interact with the database. For example, to download the data and generate a map from OpenAlex work ID to the list of IDs of cited works, you can use the following code:

```python
from hvectorspaces.io import PostgresClient

id_to_cited_ids = {}
with PostgresClient() as client:
    citation_map = client.fetch_per_decade_data(1980)
    for oa_id, refs in citation_map:
        id_to_cited_ids[oa_id] = refs
```

The `PostgresClient.fetch_per_decade_data` method can also take a list of column names as an optional argument to specify which additional columns to retrieve from the database.

In order to launch a generic SQL query against the PostgresClient, you can use the `PostgresClient.execute_query` method. For example:

```python
from hvectorspaces.io import PostgresClient
with PostgresClient() as client:
    result = client.execute_sql("SELECT * FROM openalex_vector_spaces LIMIT 10;")
    for row in result:
        print(row)
```

Your `.env` file should contain the relevant environment variables to connect to the PostgresClient DB instance. These are `PG_HOST`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, and `PG_PORT`.

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

- `create_postgresql_db.py`: Creates the `openalex_vector_spaces` table in the target PostgreSQL database and loads the corresponding CSV from the versioned `exported_db/` subdirectory. The active version is set by `DB_VERSION` at the top of the script; point `PG_DATABASE` at the matching database before running.
- `sql_upload_oa_data.py`: Fetches OpenAlex data and saves it as a gzipped CSV to `exported_db/v2/openalex_vector_spaces.csv.gz`. It searches for works containing "vector space", published after 1920 with more than 20 citations, then expands 2 hops through the citation network (same citation filter). Run this before `create_postgresql_db.py` to produce the CSV that gets loaded into Postgres.
- `add_in_decade_references_column.py`: This script adds the `in_decade_references` column to the `openalex_vector_spaces` table. It populates this column with the list of cited works that were published in the same decade as the citing work.
- `create_clusters.py`: This script uses a pre-defined clustering method (defaults to `leiden`) to create clusters of works within different decades based on their citation relationships. The script can be updated to fetch additional fields from the database as needed. It can be called from cli with

```bash
python -m scripts.create_clusters --output_path <json_path>
```
Optional arguments are: `--clustering_method` (default: `leiden`), `--decade_start` (default: 1950), `--cluster_size_cutoff` (default: 5) and `--top_n` (default: 10). Additional information can be found in the docstring of the main method.


- `create_graph_from_clusters.py`: This script generates a citation graph from the clusters created in the previous step. It constructs a directed graph where nodes represent works and edges represent citation relationships between them, divided by decades. The resulting graph is saved in a specified output path. It can be called from cli with

```bash
python -m scripts.create_graph_from_clusters --input_path <json_path> --output_path <graph_path>
```