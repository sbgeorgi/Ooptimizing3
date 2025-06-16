import sys
import subprocess
import re

REQUIRED_SIG = {
    'Total Rows Fetched': '8321',
    'Markers Created': '8302',
    'Unique Sources Found': '2',
    'Unique Project Types': '67',
    'Unique Countries Found': '152',
}

TIME_RE = re.compile(r"Total Time:\s+(\d+\.\d+)")

if len(sys.argv) != 2:
    sys.exit("Usage: python run_and_validate.py <script.py>")

script = sys.argv[1]

num_runs = 10
sum_time = 0.0
for i in range(num_runs):
    proc = subprocess.run([sys.executable, script, '--db', 'mydatabase.db'], capture_output=True, text=True)
    output = proc.stdout
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        sys.exit(1)

    for key, val in REQUIRED_SIG.items():
        pattern = rf"{re.escape(key)}:\s+(\S+)"
        m = re.search(pattern, output)
        if not m or m.group(1) != val:
            print(f"Validation failed on run {i+1}: expected '{key}: {val}', got '{m.group(0) if m else 'None'}'")
            sys.exit(1)
    m = TIME_RE.search(output)
    if not m:
        print(f"Could not find total time on run {i+1}")
        sys.exit(1)
    sum_time += float(m.group(1))

print(sum_time / num_runs)
