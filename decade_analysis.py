#!/usr/bin/env python3
"""
Script to analyze papers by decade for layer=0 from the OpenAlex vector spaces dataset.
"""

import pandas as pd
import gzip
from collections import Counter

# Read the compressed CSV file
print("Loading data from exported_db/openalex_vector_spaces.csv.gz...")
with gzip.open("exported_db/openalex_vector_spaces.csv.gz", "rt") as f:
    df = pd.read_csv(f)

print(f"Total papers in dataset: {len(df)}")

# Filter for layer=0
layer_0_df = df[df["layer"] == 0]
print(f"Papers with layer=0: {len(layer_0_df)}")

# Calculate decade from publication_year
layer_0_df = layer_0_df.copy()
layer_0_df["decade"] = (layer_0_df["publication_year"] // 10) * 10

# Count papers per decade
decade_counts = layer_0_df["decade"].value_counts().sort_index()

print("\n" + "=" * 50)
print("Papers per decade (layer=0):")
print("=" * 50)
for decade, count in decade_counts.items():
    if pd.notna(decade):
        decade_end = int(decade) + 9
        print(f"{int(decade)}-{decade_end}: {count:,} papers")

# Handle papers with missing year
missing_year = layer_0_df["publication_year"].isna().sum()
if missing_year > 0:
    print(f"Missing year: {missing_year:,} papers")

print("=" * 50)
print(f"Total: {len(layer_0_df):,} papers")
