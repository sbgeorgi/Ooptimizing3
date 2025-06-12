#!/usr/bin/env python3
"""
benchmark_update_filters_and_markers.py
--------------------------------------
Standalone benchmark harness for the most expensive part of
`/update_filters_and_markers` in GlobalRegistryFINAL.

• Runs the main SQL + marker/count post-processing outside Flask.
• Prints timings so you can adjust parameters (batch size, PRAGMA tweaks,
  pre-built indexes, etc.) and immediately see the impact.
• Optional sweep mode to compare several batch sizes in one run.

USAGE
=====

# single run (default DB_FETCH_BATCH_SIZE = 16 384)
python benchmark_update_filters_and_markers.py --db ./mydatabase.db

# sweep several batch sizes
python benchmark_update_filters_and_markers.py --db ./mydatabase.db --test-batch-sizes 4096 8192 16384 32768
"""
from __future__ import annotations
import argparse, os, sqlite3, timeit, random, json, sys
from typing import Any, Dict, List, Tuple

# --------------------------------------------------------------------------- #
#   SECTION 1 – constants you might want to play with                         #
# --------------------------------------------------------------------------- #
DB_FETCH_BATCH_SIZE_DEFAULT = 16_384

PRAGMA_SCRIPT = """
PRAGMA query_only   = TRUE;
PRAGMA journal_mode = OFF;
PRAGMA synchronous  = OFF;
PRAGMA temp_store   = MEMORY;
PRAGMA cache_size   = -196608;     /* target ~192 MB */
PRAGMA mmap_size    = 268435456;   /* 256 MB */
"""

# columns used by the live route – change only if your schema differs
PROJECT_TABLE_NAME     = "mytable"
PROJ_GEO               = "geolocation"
PROJ_EMISSIONS         = "Estimated_Annual_Emission_Reductions"
PROJ_ID                = "Project_ID"
PROJ_TYPE              = "Project_Type"
PROJ_SOURCE            = "SourceDB"
PROJ_COUNTRY           = "Country"

ESSENTIAL_COLUMNS_FOR_MARKER_QUERY = [
    PROJ_GEO, PROJ_EMISSIONS, PROJ_ID, PROJ_TYPE,
    PROJ_SOURCE, PROJ_COUNTRY,
]

# --------------------------------------------------------------------------- #
#   SECTION 2 – DB helpers                                                    #
# --------------------------------------------------------------------------- #
def get_db(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        sys.exit(f"DB not found: {db_path}")
    uri = f"file:{db_path}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=15.0, check_same_thread=False)
    conn.executescript(PRAGMA_SCRIPT)
    conn.row_factory = None  # raw tuples – fastest
    return conn

# --------------------------------------------------------------------------- #
#   SECTION 3 – minimal marker factory copied from the production route       #
# --------------------------------------------------------------------------- #
def _radius(e: float) -> float:
    if e < 51_126: return 10.0
    if e < 235_483.5677: return 16.67
    if e < 1_212_860.6667: return 23.33
    return 40.0

def create_marker_data(row: Tuple[Any, ...], col_idx: Dict[str, int]) -> Dict[str, Any] | None:
    try:
        geo = row[col_idx[PROJ_GEO]]
        if not geo or "," not in geo: return None
        emis_val = row[col_idx[PROJ_EMISSIONS]]
        pid      = row[col_idx[PROJ_ID]]
        ptype    = row[col_idx[PROJ_TYPE]]
        if emis_val is None or pid is None: return None

        lat_str, lon_str = geo.split(",", 1)
        lat = float(lat_str) + random.uniform(-1e-4, 1e-4)
        lon = float(lon_str) + random.uniform(-1e-4, 1e-4)
        radius = _radius(float(emis_val))

        return {
            "lat": lat, "lon": lon, "radius": radius,
            "project_id": pid, "project_type": ptype,
        }
    except Exception:
        return None

# --------------------------------------------------------------------------- #
#   SECTION 4 – core benchmark                                                #
# --------------------------------------------------------------------------- #
def run_benchmark(db_path: str,
                  batch_size: int = DB_FETCH_BATCH_SIZE_DEFAULT,
                  filters: Dict[str, Any] | None = None) -> Dict[str, Any]:
    filters = filters or {}
    select_cols_sql = ", ".join(f"`{c}`" for c in ESSENTIAL_COLUMNS_FOR_MARKER_QUERY)
    where_clause    = ""  # pass empty filters for worst-case baseline
    params: List[Any] = []

    sql = f"SELECT {select_cols_sql} FROM `{PROJECT_TABLE_NAME}` {where_clause}"

    conn   = get_db(db_path)
    cursor = conn.cursor()
    cursor.arraysize = batch_size

    times: Dict[str, float] = {}
    t0 = timeit.default_timer()
    cursor.execute(sql, params)
    times["main_query_execution"] = timeit.default_timer() - t0

    # precompute indexes once
    col_names = tuple(desc[0] for desc in cursor.description)
    col_idx   = {name: i for i, name in enumerate(col_names)}

    markers: List[Dict[str, Any]] = []
    t0 = timeit.default_timer()
    fetch = cursor.fetchmany
    extend = markers.extend
    while True:
        chunk = fetch(batch_size)
        if not chunk:
            break
        batch = [m for m in (create_marker_data(r, col_idx) for r in chunk) if m]
        extend(batch)
    times["marker_and_count_processing"] = timeit.default_timer() - t0
    times["total"] = sum(times.values())
    times["markers"] = len(markers)
    conn.close()
    return times

# --------------------------------------------------------------------------- #
#   SECTION 5 – CLI / sweep                                                   #
# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Benchmark for /update_filters_and_markers hot-path"
    )
    ap.add_argument("--db", required=True,
                    help="Path to SQLite database (read-only)")
    ap.add_argument("--batch-size", type=int, default=DB_FETCH_BATCH_SIZE_DEFAULT,
                    help="DB_FETCH_BATCH_SIZE to test (ignored if --test-batch-sizes given)")
    ap.add_argument("--test-batch-sizes", nargs="+", type=int,
                    help="Run a sweep of batch sizes (e.g. 4096 8192 16384)")
    args = ap.parse_args()

    batch_sizes = args.test_batch_sizes or [args.batch_size]

    print("batch_size\tmain_query_s\tproc_s\ttotal_s\tmarkers")
    for bs in batch_sizes:
        res = run_benchmark(args.db, batch_size=bs)
        print(f"{bs}\t{res['main_query_execution']:.6f}\t"
              f"{res['marker_and_count_processing']:.6f}\t"
              f"{res['total']:.6f}\t{res['markers']}")

if __name__ == "__main__":
    main()
