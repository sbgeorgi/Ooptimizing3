#!/usr/bin/env python3
"""
Standalone Benchmark Harness for 'update_filters_and_markers'

This script is a self-contained testbed designed for performance optimization.
This version represents the culmination of multiple AI-driven tournaments and
the integration of fixes from a live production environment.
"""
from __future__ import annotations
import argparse
import os
import sqlite3
import timeit
import random
from collections import defaultdict, Counter # Kept for potential future hypotheses
import sys

# =========================================================================== #
# SECTION 1: CONFIGURATION (Primary target for AI modifications)
# =========================================================================== #

# --- Database Fetch Size ---
DB_FETCH_BATCH_SIZE = 16384

# --- Database PRAGMA Settings ---
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
PROJ_NAME               = "Project_Name"
PROJ_TYPE               = "Project_Type"
PROJ_SOURCE             = "SourceDB"
PROJ_COUNTRY            = "Country"
PROJ_GEO                = "geolocation"
PROJ_EMISSIONS          = "Estimated_Annual_Emission_Reductions"
PROJ_START_VERRA_GS     = "Crediting_Period_Start_Date"

# --- Filters to Simulate a User Request ---
FILTERS_TO_TEST = {
    'sources': ['Verra', 'GoldStandard'],
}


# =========================================================================== #
# SECTION 2: CORE LOGIC (Primary target for AI modifications)
# =========================================================================== #

def get_db(db_path: str, pragma_script: str) -> sqlite3.Connection:
    """Establishes a read-only database connection with specified PRAGMAs."""
    if not os.path.exists(db_path):
        sys.exit(f"ERROR: Database file not found at '{db_path}'")
    db_uri = f"file:{db_path}?mode=ro&immutable=1"
    conn = sqlite3.connect(db_uri, uri=True, timeout=15.0)
    conn.executescript(pragma_script)
    # The benchmark environment assumes raw tuples for maximum performance,
    # unlike the production environment which may use sqlite3.Row.
    conn.row_factory = None
    return conn

def build_filter_query(filters: dict) -> tuple[str, list]:
    """Builds a SQL query from a filter dictionary."""
    base_query = f"SELECT * FROM `{PROJECT_TABLE_NAME}`"
    where_conditions = []
    params = []

    if sources := filters.get('sources'):
        placeholders = ','.join(['?'] * len(sources))
        where_conditions.append(f"`{PROJ_SOURCE}` IN ({placeholders}) COLLATE NOCASE")
        params.extend(sources)
    # ... other filter conditions would go here ...

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

# NOTE: The create_marker_data function has been intentionally removed.
# Its logic has been inlined into run_full_benchmark for performance.

# =========================================================================== #
# SECTION 3: BENCHMARK EXECUTION
# =========================================================================== #

def run_full_benchmark(db_path: str, pragma_script: str, batch_size: int, filters: dict):
    """
    Executes the full benchmark process, now with inlined marker creation.
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

    # --- Data Fetching and Processing (FULLY OPTIMIZED) ---
    col_names = tuple(desc[0] for desc in cursor.description)
    col_idx = {name: i for i, name in enumerate(col_names)}

    markers = []
    source_counts = {}
    project_type_counts = {}
    country_counts = {}
    total_rows_fetched = 0

    t0_processing = timeit.default_timer()

    # Cache methods and values outside the hot loop
    append_marker = markers.append
    rand = random.random
    
    # Pre-compute indices to avoid dict lookups inside the loop
    source_idx = col_idx.get(PROJ_SOURCE)
    type_idx = col_idx.get(PROJ_TYPE)
    country_idx = col_idx.get(PROJ_COUNTRY)
    geo_idx = col_idx.get(PROJ_GEO)
    emis_idx = col_idx.get(PROJ_EMISSIONS)
    id_idx = col_idx.get(PROJ_ID)
    
    for row in cursor:
        total_rows_fetched += 1
        
        # --- START: Inlined create_marker_data logic ---
        # This structure mirrors the final, stable production code.
        try:
            geolocation = row[geo_idx]
            emissions_val = row[emis_idx]

            # Basic validation
            if not (geolocation and ',' in geolocation):
                # Skip counting for this row if it can't be a marker
                continue
            if emissions_val is None:
                continue

            # This try-except is crucial for handling non-numeric data
            emissions_float = float(emissions_val)
            
            # Core marker creation logic
            lat_str, _, lon_str = geolocation.partition(',') # Faster than split
            jitter = rand() * 2e-4 - 1e-4             # Faster than uniform
            
            # The structure must match what run_and_validate.py expects
            append_marker({
                'lat': float(lat_str) + jitter,
                'lon': float(lon_str) + jitter,
                'radius': _radius(emissions_float),
                'project_id': row[id_idx],
                'project_type': row[type_idx],
            })
        except (ValueError, TypeError, KeyError, IndexError):
            # Safely skip any row that causes a data processing error
            pass
        # --- END: Inlined logic ---

        # Perform counting using the original, functional dict .get() pattern.
        if source_idx is not None and (val := row[source_idx]):
            source_counts[val] = source_counts.get(val, 0) + 1
        
        if type_idx is not None and (val := row[type_idx]):
            project_type_counts[val] = project_type_counts.get(val, 0) + 1
        
        if country_idx is not None and (val := row[country_idx]):
            country_counts[val] = country_counts.get(val, 0) + 1
    
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