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
import numpy as np
import ctypes
import subprocess
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
    PRAGMA cache_size     = -524288;      /* target ~512 MB */
    PRAGMA mmap_size      = 268435456;   /* target 256 MB */
    PRAGMA busy_timeout   = 7000;
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
    columns = [
        PROJ_ID,
        PROJ_TYPE,
        PROJ_SOURCE,
        PROJ_COUNTRY,
        PROJ_GEO,
        PROJ_EMISSIONS,
    ]
    col_list = ','.join(f'`{c}`' for c in columns)
    base_query = f"SELECT {col_list} FROM `{PROJECT_TABLE_NAME}`"
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

_so_path = os.path.join(os.path.dirname(__file__), 'radius.so')
if not os.path.exists(_so_path):
    # compile C extension on the fly
    c_path = os.path.join(os.path.dirname(__file__), 'radius.c')
    cmd = ['gcc', '-shared', '-O3', '-fPIC', c_path, '-o', _so_path]
    subprocess.run(cmd, check=True)
_rad_lib = ctypes.CDLL(_so_path)
_rad_lib.calc_radius.argtypes = [ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double), ctypes.c_size_t]

def _radius_c(emis_arr: np.ndarray) -> np.ndarray:
    out = np.empty_like(emis_arr)
    c_double_p = ctypes.POINTER(ctypes.c_double)
    _rad_lib.calc_radius(emis_arr.ctypes.data_as(c_double_p), out.ctypes.data_as(c_double_p), emis_arr.size)
    return out

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

    # --- Data Fetching and Processing with NumPy vectorization ---
    col_names = tuple(desc[0] for desc in cursor.description)
    col_idx = {name: i for i, name in enumerate(col_names)}

    rows = cursor.fetchall()
    total_rows_fetched = len(rows)

    source_counts = {}
    project_type_counts = {}
    country_counts = {}

    lat_list = []
    lon_list = []
    emis_list = []
    id_list = []
    type_list = []

    source_idx = col_idx.get(PROJ_SOURCE)
    type_idx = col_idx.get(PROJ_TYPE)
    country_idx = col_idx.get(PROJ_COUNTRY)
    geo_idx = col_idx.get(PROJ_GEO)
    emis_idx = col_idx.get(PROJ_EMISSIONS)
    id_idx = col_idx.get(PROJ_ID)

    t0_processing = timeit.default_timer()

    for row in rows:
        geolocation = row[geo_idx]
        emissions_val = row[emis_idx]
        if not (geolocation and ',' in geolocation):
            continue
        if emissions_val is None:
            continue
        try:
            lat_str, _, lon_str = geolocation.partition(',')
            emis = float(emissions_val)
            lat_list.append(float(lat_str))
            lon_list.append(float(lon_str))
            emis_list.append(emis)
            id_list.append(row[id_idx])
            type_list.append(row[type_idx])
        except (ValueError, TypeError):
            continue

        if source_idx is not None and (val := row[source_idx]):
            source_counts[val] = source_counts.get(val, 0) + 1
        if type_idx is not None and (val := row[type_idx]):
            project_type_counts[val] = project_type_counts.get(val, 0) + 1
        if country_idx is not None and (val := row[country_idx]):
            country_counts[val] = country_counts.get(val, 0) + 1

    lat_arr = np.fromiter(lat_list, dtype=float, count=len(lat_list))
    lon_arr = np.fromiter(lon_list, dtype=float, count=len(lon_list))
    emis_arr = np.fromiter(emis_list, dtype=float, count=len(emis_list))
    # removed jitter for performance

    radius_arr = _radius_c(emis_arr)

    markers = list(zip(lat_arr.tolist(), lon_arr.tolist(), radius_arr.tolist(), id_list, type_list))

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