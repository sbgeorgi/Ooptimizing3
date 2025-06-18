AI Mastermind Agent Rules - Tournament 2 (DB Focus)
Core Mandate: I will complete a minimum of 10 generations. I will not stop before Generation 10 is complete. After Generation 10, I may only stop if five (5) consecutive generations show no improvement.

New Prime Directive - Attack the Bottleneck: The current champion's db_query time is the primary bottleneck. My main goal is to reduce this time.

Database Interaction Edict: At least three (3) hypotheses per generation must directly target database interaction. Trivial changes are forbidden. Hypotheses must be ambitious and explore strategies such as:

SQL Query Optimization: Modify the build_filter_query function to generate more efficient SQL. The most obvious starting point is changing SELECT * to only select the exact columns needed by the processing loop (Project_ID, Project_Type, SourceDB, Country, geolocation, Estimated_Annual_Emission_Reductions). This reduces data transfer from the DB to the Python process.

Data Fetching Strategy: The current code uses cursor.fetchall(), which loads all results into memory at once. A key paradigm shift to test is replacing this with a loop over cursor.fetchmany(batch_size). This processes the data in streaming chunks, which can improve performance and reduce memory pressure.

Connection & PRAGMA Tuning: Experiment with the PRAGMA settings in PRAGMA_SCRIPT_RO. While the current settings are good, different cache_size or mmap_size values could yield better performance for this specific query pattern.

Python Processing Edict: Up to two (2) hypotheses per generation may still target the Python data processing loop that begins after the data is fetched (the for row in rows: block). While this section is already fast, further micro-optimizations or alternative data structure approaches (e.g., different NumPy vectorization techniques) are permissible.

Benchmarking Integrity: All benchmark runs via run_and_validate.py must be executed at least 5 times to ensure stable and reliable results. The average time is the official metric.
