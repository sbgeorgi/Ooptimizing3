#!/usr/bin/env python3
"""
Advanced Standalone Benchmark Harness for 'update_filters_and_markers'

This script is a self-contained, enhanced testbed designed for a sophisticated
AI-driven optimization tournament. It has been re-engineered to provide a
flexible platform for testing a wide range of database optimization strategies.

Key enhancements include:
1.  **In-Memory Database Cloning:** Each run uses a fresh in-memory clone of
    the database, ensuring test isolation and allowing for schema modifications
    like index creation without altering the source file.
2.  **Config-Driven Experiments:** A single `BENCHMARK_CONFIG` dictionary drives
    the entire test, making it trivial for an AI to formulate hypotheses by
    modifying parameters for column selection, fetch strategy, and indexing.
3.  **Query Plan Analysis:** Automatically runs and prints the output of
    `EXPLAIN QUERY PLAN` for every generated query, giving the AI crucial
    diagnostic data beyond simple timing.
4.  **Flexible Fetch Strategies:** Natively supports 'fetchall', 'fetchmany',
    and direct 'iterate' strategies, selectable via the config.
"""
from __future__ import annotations
import argparse
import os
import sqlite3
import timeit
import random
import sys

# =========================================================================== #
# SECTION 1: CONFIGURATION (Primary target for AI modifications)
# =========================================================================== #

# This dictionary is the central control panel for the AI agent.
# By modifying this single object, the agent can test all key hypotheses.
BENCHMARK_CONFIG = {
    # --- STRATEGY 1: Column Selection ---
    # Test the impact of selecting specific columns vs. `SELECT *`.
    # An empty list `[]` will default to `SELECT *`.
    # A good starting hypothesis is to select only the columns needed in the loop.
    "columns_to_select": [
        "Project_ID", "Project_Type", "SourceDB", "Country",
        "geolocation", "Estimated_Annual_Emission_Reductions"
    ],

    # --- STRATEGY 2: Data Fetching Method ---
    # Test different ways of retrieving data from the cursor.
    # Options: 'iterate', 'fetchall', 'fetchmany'
    "fetch_strategy": 'iterate',
    "db_fetch_batch_size": 16384,  # Only used if fetch_strategy is 'fetchmany'

    # --- STRATEGY 3: Indexing ---
    # Test the impact of creating indexes before the query runs.
    # The agent can add 'CREATE INDEX...' statements here.
    # The benchmark harness will create a fresh in-memory DB for each run,
    # so these indexes are temporary and don't affect the source file.
    "db_setup_statements": [
        # Example Hypothesis: "CREATE INDEX idx_source ON mytable(SourceDB COLLATE NOCASE)"
    ],

    # --- STRATEGY 4: PRAGMA Tuning ---
    # Test different PRAGMA settings for read-only operations.
    "pragma_script_ro": """
        PRAGMA query_only     = TRUE;
        PRAGMA journal_mode   = OFF;
        PRAGMA synchronous    = OFF;
        PRAGMA temp_store     = MEMORY;
        PRAGMA cache_size     = -196608;
        PRAGMA mmap_size      = 268435456;
    """,
}

# --- Table and Column Names (Schema-dependent, should not be changed by AI) ---
PROJECT_TABLE_NAME = "mytable"
PROJ_ID = "Project_ID"
PROJ_TYPE = "Project_Type"
PROJ_SOURCE = "SourceDB"
PROJ_COUNTRY = "Country"
PROJ_GEO = "geolocation"
PROJ_EMISSIONS = "Estimated_Annual_Emission_Reductions"

# --- Filters to Simulate a User Request (Static for consistent tests) ---
FILTERS_TO_TEST = {
    'sources': ['Verra', 'GoldStandard'],
}


# =========================================================================== #
# SECTION 2: CORE LOGIC (Modified to support advanced testing)
# =========================================================================== #

def setup_in_memory_db(source_db_path: str, pragma_script: str, setup_statements: list[str]) -> sqlite3.Connection:
    """
    Creates an isolated, in-memory database for a single benchmark run.
    It copies the content from the source DB, applies PRAGMAs, and runs setup SQL.
    """
    if not os.path.exists(source_db_path):
        sys.exit(f"ERROR: Source database file not found at '{source_db_path}'")

    # 1. Create the destination in-memory database
    mem_conn = sqlite3.connect(":memory:")

    # 2. Copy the source database content to the in-memory database
    try:
        # Attach the source DB and dump its content to the in-memory DB
        mem_conn.execute(f"ATTACH DATABASE '{source_db_path}' AS source_db")
        # Use backup API for a robust copy
        backup_conn = sqlite3.connect(source_db_path, timeout=15.0)
        backup_conn.backup(mem_conn)
        backup_conn.close()
        mem_conn.execute("DETACH DATABASE source_db")
    except sqlite3.Error as e:
        mem_conn.close()
        sys.exit(f"ERROR: Failed to clone database to memory: {e}")

    # 3. Apply PRAGMA settings to the new in-memory database
    mem_conn.executescript(pragma_script)

    # 4. Run any pre-benchmark setup statements (e.g., CREATE INDEX)
    if setup_statements:
        print("--- Applying Setup SQL ---")
        for stmt in setup_statements:
            print(f"Executing: {stmt}")
            mem_conn.execute(stmt)
        print("-" * 28)

    # Use raw tuples for maximum performance in the benchmark
    mem_conn.row_factory = None
    return mem_conn


def build_filter_query(filters: dict, columns: list[str]) -> tuple[str, list]:
    """Builds a SQL query, now with dynamic column selection."""
    select_clause = ", ".join(f"`{c}`" for c in columns) if columns else "*"
    base_query = f"SELECT {select_clause} FROM `{PROJECT_TABLE_NAME}`"
    where_conditions = []
    params = []

    if sources := filters.get('sources'):
        placeholders = ','.join(['?'] * len(sources))
        where_conditions.append(f"`{PROJ_SOURCE}` IN ({placeholders}) COLLATE NOCASE")
        params.extend(sources)

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


# =========================================================================== #
# SECTION 3: BENCHMARK EXECUTION (Heavily modified for new capabilities)
# =========================================================================== #

def run_full_benchmark(db_path: str, config: dict, filters: dict):
    """
    Executes the full, enhanced benchmark process.
    """
    print("--- Starting Benchmark ---")
    print(f"Source Database: {db_path}")
    print(f"Fetch Strategy:  {config.get('fetch_strategy', 'N/A')}")
    print(f"Batch Size:      {config.get('db_fetch_batch_size', 'N/A')}")
    print(f"Columns Selected: {len(config.get('columns_to_select', [])) or 'All (*)'}")
    print("-" * 28)

    # --- Setup ---
    # Create a fresh, isolated in-memory DB for this run
    conn = setup_in_memory_db(
        source_db_path=db_path,
        pragma_script=config.get('pragma_script_ro', ''),
        setup_statements=config.get('db_setup_statements', [])
    )
    cursor = conn.cursor()
    cursor.arraysize = config.get('db_fetch_batch_size', 16384)
    sql_query, params = build_filter_query(filters, config.get('columns_to_select', []))

    # --- Query Plan Analysis ---
    print("--- Query Plan Analysis ---")
    try:
        plan_query = f"EXPLAIN QUERY PLAN {sql_query}"
        cursor.execute(plan_query, params)
        plan = cursor.fetchall()
        print(f"SQL: {sql_query}")
        print("Plan:")
        for row in plan:
            print(f"  > {row[3]}")
    except sqlite3.Error as e:
        print(f"Could not execute EXPLAIN QUERY PLAN: {e}")
    print("-" * 28)

    # --- Query Execution Timing ---
    t0_query = timeit.default_timer()
    cursor.execute(sql_query, params)
    query_execution_time = timeit.default_timer() - t0_query

    # --- Data Fetching and Processing ---
    # This section now dynamically handles different fetch strategies
    t0_processing = timeit.default_timer()

    col_names = tuple(desc[0] for desc in cursor.description)
    col_idx = {name: i for i, name in enumerate(col_names)}

    markers = []
    source_counts = {}
    project_type_counts = {}
    country_counts = {}
    total_rows_fetched = 0

    append_marker = markers.append
    rand = random.random
    source_idx = col_idx.get(PROJ_SOURCE)
    type_idx = col_idx.get(PROJ_TYPE)
    country_idx = col_idx.get(PROJ_COUNTRY)
    geo_idx = col_idx.get(PROJ_GEO)
    emis_idx = col_idx.get(PROJ_EMISSIONS)
    id_idx = col_idx.get(PROJ_ID)

    def process_row(row):
        nonlocal total_rows_fetched
        total_rows_fetched += 1
        try:
            # Marker creation logic
            geolocation = row[geo_idx]
            emissions_val = row[emis_idx]
            if not (geolocation and ',' in geolocation and emissions_val is not None):
                return
            emissions_float = float(emissions_val)
            lat_str, _, lon_str = geolocation.partition(',')
            jitter = rand() * 2e-4 - 1e-4
            append_marker({
                'lat': float(lat_str) + jitter, 'lon': float(lon_str) + jitter,
                'radius': _radius(emissions_float), 'project_id': row[id_idx],
                'project_type': row[type_idx],
            })
        except (ValueError, TypeError, KeyError, IndexError):
            pass  # Safely skip rows with processing errors

        # Counting logic
        if source_idx is not None and (val := row[source_idx]):
            source_counts[val] = source_counts.get(val, 0) + 1
        if type_idx is not None and (val := row[type_idx]):
            project_type_counts[val] = project_type_counts.get(val, 0) + 1
        if country_idx is not None and (val := row[country_idx]):
            country_counts[val] = country_counts.get(val, 0) + 1

    fetch_strategy = config.get('fetch_strategy', 'iterate')
    if fetch_strategy == 'fetchall':
        rows = cursor.fetchall()
        for row in rows:
            process_row(row)
    elif fetch_strategy == 'fetchmany':
        while True:
            rows = cursor.fetchmany(cursor.arraysize)
            if not rows:
                break
            for row in rows:
                process_row(row)
    else:  # 'iterate' is the default
        for row in cursor:
            process_row(row)

    processing_time = timeit.default_timer() - t0_processing
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
        description="Run an advanced benchmark test for database optimization.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the source SQLite database file."
    )
    args = parser.parse_args()

    # The AI agent's primary task is to modify the BENCHMARK_CONFIG dictionary
    # above and then execute this script.
    run_full_benchmark(
        db_path=args.db,
        config=BENCHMARK_CONFIG,
        filters=FILTERS_TO_TEST
    )