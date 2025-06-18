# AI Mastermind Agent Rules - Tournament 2

Core Mandate: I will complete a minimum of 10 generations. I will not stop before Generation 10 is complete. After Generation 10, I may only stop if five (5) consecutive generations show no improvement.

Hypothesis Edict - Data Processing Focus: The previous champion optimized database fetching. Therefore, all five of my hypotheses per generation must target the primary data processing loop (the for row in cursor.execute(...) block) and the create_marker_data function. Trivial changes are forbidden. Hypotheses must be ambitious and well-reasoned.

Paradigm Shift Edict: True performance breakthroughs often come from new approaches.[1][2] At least two hypotheses per generation must explore fundamentally different paradigms. Examples include:

Parallel Processing: Utilizing multiprocessing or concurrent.futures to distribute the workload across multiple CPU cores.[3][4][5]

C-Extension: Offloading the most computationally intensive parts of the create_marker_data function to a C extension using ctypes.[6][7][8]

JIT Compilation: Applying a Just-In-Time compiler like Numba to the data processing functions.[2][9]

Vectorization: If applicable, refactoring the processing logic to use NumPy for vectorized operations, which are significantly faster than Python loops for numerical data.[9][10]

Advanced Optimization Edict: At least two hypotheses per generation must explore advanced, in-process optimization techniques, such as:

Algorithmic and Data Structure Improvements: Replace existing data structures with more performant alternatives (e.g., using sets for fast membership testing, or __slots__ to reduce memory overhead).[1][11][12]

Function Caching: Employ memoization with functools.lru_cache on functions that are called repeatedly with the same arguments.[11][13]

I/O and Deserialization Optimization: Experiment with faster JSON parsing libraries like orjson if applicable.

Benchmarking Integrity: All benchmark runs via run_and_validate.py must be executed multiple times to ensure stable and reliable results.[14][15] The average time will be the official metric for comparison. The validation script should also "warm up" the code before timing to account for initial overhead.[16]

Creative Exploration: Be innovative. Think beyond standard library solutions and investigate cutting-edge, third-party libraries known for high performance in specific domains (e.g., data manipulation, numerical computation).
