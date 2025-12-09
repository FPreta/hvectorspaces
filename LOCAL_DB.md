# Local PostgreSQL Database Setup

This document explains how to set up a **local PostgreSQL database** that mirrors the data we previously hosted on **CockroachDB**.

The local DB contains two tables:

- `openalex_vector_spaces`
- `per_decade_citation_graph`

All data is stored locally in PostgreSQL and loaded from compressed CSV files that are **versioned in this repo** using Git LFS.

---

## 1. Where the data lives in this repo

The Cockroach → PostgreSQL export lives in:

```text
exported_db/
├── schema.sql
├── openalex_vector_spaces.csv.gz
└── per_decade_citation_graph.csv.gz
```

The exported database files inside the `exported_db/` directory are large (.csv.gz).
To avoid bloating the Git repository and to make downloads efficient, we store them using Git LFS (Large File Storage).

### 1.1 Install Git LFS

Download and install Git LFS by following the instructions at https://git-lfs.github.com/ or run:
```bash
curl -s https://packagecloud.io/github/git-lfs/install.sh | sudo bash
```

Enable Git LFS:

```bash
git lfs install
```

### 1.2. Only during setup: configure Git LFS to track the exported CSV files 

From the project root:

```bash
git lfs track "exported_db/*.csv.gz"
```

This creates/updates `.gitattributes`. Commit it:

```bash
git add .gitattributes
git commit -m "Configure Git LFS for exported CSV files"
```

From the project root, add the exported CSV files to Git:
```bash
git add exported_db/*.csv.gz
git commit -m "Add exported Postgres CSV exports via Git LFS"
git push
```

Git LFS uploads the files to LFS storage automatically.

### 1.3. Collaborators: Retrieve the data with Git LFS

Before cloning or pulling:

```bash
git lfs install
```

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

## 2. Requirements

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

Python 3.10+ and a virtual environment

From the project root, after creating appropriate environment:

```bash

pip install -e .           # or: pip install -r requirements.txt
```

This should install `psycopg2`, `python-dotenv`, and the `hvectorspaces` package (or however this repo is structured).

## 3. Create the local PostgreSQL database

We assume a database name like `hvectorspaces` (you can change it, but then update .env accordingly).

From a terminal:

```bash
createdb hvectorspaces

```


Or from psql:

```sql
CREATE DATABASE hvectorspaces;

```

## 4. Configure environment variables

The Python clients expect standard PostgreSQL env vars. Create a .env file in the project root or modify the existing one with the following variables:

```env


PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=hvectorspaces
PG_USER=<your_mac_username>
PG_PASSWORD=
```

Notes:

`PG_USER` should normally be your macOS username (the output of the terminal command `whoami`).

For Postgres.app defaults, no password is required for local connections, so you can leave `PG_PASSWORD` empty.

## 5. Create the tables

In order to create the tables, run the following command from the terminal:

```bash
psql -d hvectorspaces -f exported_db/schema.sql
```

You can verify the tables exist by running

```bash
psql -d hvectorspaces -c "\d+ openalex_vector_spaces"
psql -d hvectorspaces -c "\d+ per_decade_citation_graph"
```

## 6. Load the data from the compressed CSVs

We have a Python script that uses the local PostgreSQL client (PostgresClient) to load the .csv.gz files via COPY.

From the project root:

```bash
python -m scripts.create_postgresql_db
```

This script should:

Connect to PostgreSQL using the env vars in .env.

Load:

`exported_db/openalex_vector_spaces.csv.gz` → `openalex_vector_spaces`

`exported_db/per_decade_citation_graph.csv.gz` → `per_decade_citation_graph`

If everything works, you should see log output indicating that the rows were loaded.