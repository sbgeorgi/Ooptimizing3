#!/usr/bin/env python3
"""
Standalone Benchmark Harness for 'update_filters_and_markers'

This script is a self-contained testbed designed for performance optimization,
particularly through interaction with an AI code assistant like OpenAI Codex.

PURPOSE:
To provide a simple, editable, and runnable environment that isolates the
core logic of the `update_filters_and_markers` endpoint. An AI can be
instructed to modify this file to test performance hypotheses.

HOW TO USE WITH AN AI:
1. Provide the entire script to the AI as context.
2. Instruct the AI to modify a specific part of the code.
   - "Modify the FILTERS_TO_TEST dictionary to simulate a complex search."
   - "In PRAGMA_SCRIPT_RO, change the cache_size to -262144 and remove mmap_size."
   - "Rewrite the processing loop in `run_full_benchmark` to be more efficient."
3. Run the modified script from your terminal and observe the change in timings.

Example command to run:
python benchmark_script.py --db ./path/to/your/database.db
"""
from __future__ import annotations
import argparse
import os
import sqlite3
import timeit
import random
from collections import defaultdict, Counter
import sys

# =========================================================================== #
# SECTION 1: CONFIGURATION (Primary target for AI modifications)
#
# Instruct the AI to change these values to test different scenarios.
# =========================================================================== #

# --- Database Fetch Size ---
# Tell the AI: "Set DB_FETCH_BATCH_SIZE to 8192 and run the benchmark."
DB_FETCH_BATCH_SIZE = 16384

# --- Database PRAGMA Settings ---
# Tell the AI: "Modify the PRAGMA_SCRIPT_RO to set journal_mode to WAL."
PRAGMA_SCRIPT_RO = """
    PRAGMA query_only     = TRUE;
    PRAGMA journal_mode   = OFF;
    PRAGMA synchronous    = OFF;
    PRAGMA temp_store     = MEMORY;
    PRAGMA cache_size     = -196608;      /* target ~192 MB */
    PRAGMA mmap_size      = 268435456;   /* target 256 MB */
"""

# --- Table and Column Names (Must match your schema) ---
PROJECT_TABLE_NAME      = "mytable"
PROJ_ID                 = "Project_ID"
PROJ_NAME               = "Project_Name" # Needed for search
PROJ_TYPE               = "Project_Type"
PROJ_SOURCE             = "SourceDB"
PROJ_COUNTRY            = "Country"
PROJ_GEO                = "geolocation"
PROJ_EMISSIONS          = "Estimated_Annual_Emission_Reductions"
PROJ_START_VERRA_GS     = "Crediting_Period_Start_Date" # Example name

# --- Filters to Simulate a User Request ---
# Tell the AI: "Change FILTERS_TO_TEST to an empty dictionary to test the baseline."
# Or: "Add a 'search' key with value 'reforestation' to FILTERS_TO_TEST."
FILTERS_TO_TEST = {
    'sources': ['Verra', 'GoldStandard'],
}


# =========================================================================== #
# SECTION 2: CORE LOGIC (Copied from application for realistic testing)
#
# The AI can be asked to optimize or rewrite these functions.
# =========================================================================== #

def get_db(db_path: str, pragma_script: str) -> sqlite3.Connection:
    """Establishes a read-only database connection with specified PRAGMAs."""
    if not os.path.exists(db_path):
        sys.exit(f"ERROR: Database file not found at '{db_path}'")
    db_uri = f"file:{db_path}?mode=ro&immutable=1"
    conn = sqlite3.connect(db_uri, uri=True, timeout=15.0)
    conn.executescript(pragma_script)
    conn.row_factory = None  # Use raw tuples for maximum performance
    return conn

def build_filter_query(filters: dict) -> tuple[str, list]:
    """
    Builds a SQL query from a filter dictionary. This is a simplified but
    representative version of the production query builder.
    """
    base_query = f"SELECT * FROM `{PROJECT_TABLE_NAME}`"
    where_conditions = []
    params = []

    if sources := filters.get('sources'):
        placeholders = ','.join(['?'] * len(sources))
        where_conditions.append(f"`{PROJ_SOURCE}` IN ({placeholders}) COLLATE NOCASE")
        params.extend(sources)

    if countries := filters.get('countries'):
        placeholders = ','.join(['?'] * len(countries))
        where_conditions.append(f"`{PROJ_COUNTRY}` IN ({placeholders}) COLLATE NOCASE")
        params.extend(countries)
        
    if types := filters.get('project_types'):
        placeholders = ','.join(['?'] * len(types))
        where_conditions.append(f"`{PROJ_TYPE}` IN ({placeholders}) COLLATE NOCASE")
        params.extend(types)

    if search_term := filters.get('search'):
        like_pattern = f"%{search_term}%"
        searchable_columns = [PROJ_ID, PROJ_NAME, PROJ_TYPE, PROJ_COUNTRY]
        search_clauses = " OR ".join([f"`{c}` LIKE ? COLLATE NOCASE" for c in searchable_columns if c])
        where_conditions.append(f"({search_clauses})")
        params.extend([like_pattern] * len(searchable_columns))

    if start_year := filters.get('start_year'):
        if end_year := filters.get('end_year'):
            if PROJ_START_VERRA_GS:
                condition = f"CAST(substr(trim(`{PROJ_START_VERRA_GS}`), 1, 4) AS INTEGER) BETWEEN ? AND ?"
                where_conditions.append(condition)
                params.extend([start_year, end_year])

    final_query = base_query
    if where_conditions:
        final_query += " WHERE " + " AND ".join(where_conditions)
    
    return final_query, params

def _radius(e: float) -> float:
    """Helper function to calculate marker radius based on emissions."""
    if e < 51_126: return 10.0
    if e < 235_483.5677: return 16.67
    if e < 1_212_860.6667: return 23.33
    return 40.0

def create_marker_data(row: tuple, col_idx: dict) -> dict | None:
    """Creates a marker dictionary from a database row tuple."""
    try:
        geo = row[col_idx[PROJ_GEO]]
        if not geo or "," not in geo: return None
        emis_val = row[col_idx[PROJ_EMISSIONS]]
        if emis_val is None: return None

        lat_str, lon_str = geo.split(",", 1)
        return {
            "lat": float(lat_str) + random.uniform(-1e-4, 1e-4),
            "lon": float(lon_str) + random.uniform(-1e-4, 1e-4),
            "radius": _radius(float(emis_val)),
            "project_id": row[col_idx[PROJ_ID]],
            "project_type": row[col_idx[PROJ_TYPE]],
        }
    except (ValueError, TypeError, KeyError, IndexError):
        return None

# =========================================================================== #
# SECTION 3: BENCHMARK EXECUTION
# =========================================================================== #

def run_full_benchmark(db_path: str, pragma_script: str, batch_size: int, filters: dict):
    """
    Executes the full benchmark process:
    1. Connects to the DB.
    2. Builds and executes the SQL query.
    3. Fetches and processes all results in batches.
    4. Prints a summary of timings and results.
    """
    print("--- Starting Benchmark ---")
    print(f"Database: {db_path}")
    print(f"Batch Size: {batch_size}")
    print(f"Filters: {filters if filters else 'None'}")
    print("-" * 28)

    # --- Setup and Query Execution ---
    conn = get_db(db_path, pragma_script)
    cursor = conn.cursor()
    cursor.arraysize = batch_size

    sql_query, params = build_filter_query(filters)
    
    t0 = timeit.default_timer()
    cursor.execute(sql_query, params)
    query_execution_time = timeit.default_timer() - t0

    # --- Data Fetching and Processing ---
    col_names = tuple(desc[0] for desc in cursor.description)
    col_idx = {name: i for i, name in enumerate(col_names)}

    source_idx = col_idx.get(PROJ_SOURCE)
    type_idx = col_idx.get(PROJ_TYPE)
    country_idx = col_idx.get(PROJ_COUNTRY)

    markers = []
    source_counts = {}
    project_type_counts = {}
    country_counts = {}
    total_rows_fetched = 0

    t0 = timeit.default_timer()
    sc = source_counts
    tc = project_type_counts
    cc = country_counts
    append_marker = markers.append
    for row in cursor:
        total_rows_fetched += 1
        if source_idx is not None:
            src = row[source_idx]
            if src:
                sc[src] = sc.get(src, 0) + 1
        if type_idx is not None:
            t = row[type_idx]
            if t:
                tc[t] = tc.get(t, 0) + 1
        if country_idx is not None:
            c = row[country_idx]
            if c:
                cc[c] = cc.get(c, 0) + 1
        m = create_marker_data(row, col_idx)
        if m:
            append_marker(m)
    
    processing_time = timeit.default_timer() - t0
    conn.close()

    # --- Print Results ---
    total_time = query_execution_time + processing_time
    print("--- Results ---")
    print(f"Query Execution Time:   {query_execution_time:.6f} s")
    print(f"Data Processing Time:     {processing_time:.6f} s")
    print(f"Total Time:               {total_time:.6f} s")
    print("-" * 28)
    print(f"Total Rows Fetched:       {total_rows_fetched}")
    print(f"Markers Created:          {len(markers)}")
    print(f"Unique Sources Found:     {len(source_counts)}")
    print(f"Unique Project Types:     {len(project_type_counts)}")
    print(f"Unique Countries Found:   {len(country_counts)}")
    print("--- Benchmark Finished ---\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a benchmark test for the 'update_filters_and_markers' logic.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the SQLite database file."
    )
    args = parser.parse_args()

    # Execute the benchmark using the configurations from Section 1
    run_full_benchmark(
        db_path=args.db,
        pragma_script=PRAGMA_SCRIPT_RO,
        batch_size=DB_FETCH_BATCH_SIZE,
        filters=FILTERS_TO_TEST
    )
