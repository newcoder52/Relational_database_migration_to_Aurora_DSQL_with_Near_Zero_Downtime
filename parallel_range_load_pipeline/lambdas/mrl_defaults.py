#!/usr/bin/env python3
"""mrl_defaults.py - the SINGLE source of MRL's built-in sizing/parallelism defaults
(Amendment 10 A.2).

MRL sizes its own per-task Glue jobs and parallelism from (highest first):
    start-execution input  >  fleet-row column  >  these built-in defaults.
MRL no longer reads any of these from config/params.csv (there is ONE table per DMS task, so no
"load big" sizing and no map_max_concurrency are needed; the fleet file carries parallelism). The
values below are exactly the ones that were EFFECTIVE when the fleet row is blank (the original
parser's OPTIONAL_DEFAULTS), so a blank fleet row changes nothing vs the previous behaviour.

Pure data, no imports, no AWS - importable by the Lambda, the README generator and the tests.
"""

# Range-load job sizing + parallelism (the dedicated per-task <project>-<suffix>-mrl-load job).
LOAD_WORKER_TYPE = "G.4X"          # was config/params.csv load_worker_type (G.4X)
LOAD_NUM_WORKERS = 10              # was load_num_workers (10)
LOAD_TIMEOUT_MINUTES = 2880        # was load_timeout_minutes (2880)
MAX_FILES_IN_PARALLEL = 30         # was max_files_in_parallel (30)
WRITERS_PER_FILE = 8               # was writers_per_file (8)
MAX_CONCURRENCY = 3                # parallel range loads per table (MRL's effective-when-blank
                                   # default at dffc7c5; NOT the base parser's map_max_concurrency=6)

# The reused discovery job + the count Glue job keep the original effective sizing (one table per
# task, so no load-big path is exercised; these feed the REUSED discovery / count only).
DISCOVERY_WORKER_TYPE = "G.2X"
DISCOVERY_NUM_WORKERS = 5
DISCOVERY_TIMEOUT_MINUTES = 480
# Count Glue job (python-shell) effective sizing/capacity today.
COUNT_WORKER_TYPE = "G.8X"         # validate_worker_type default (count job's effective sizing)
COUNT_NUM_WORKERS = 10             # validate_num_workers default
COUNT_TIMEOUT_MINUTES = 2880       # validate_timeout_minutes default

# Connection budget used by the capacity arithmetic (identity/connection, still from params.csv if
# set; this is the fallback only).
CONN_BUDGET = 900

# A13-A.2: fail-FAST wall-clock timeouts for the .sync Glue count/probe states, so a no-internet
# pip hang (or any stuck run) fails in minutes with a clear error instead of hanging ~20 min+.
# Overridable per task via the execution input / fleet row (probeTimeoutSeconds/countTimeoutSeconds).
PROBE_TIMEOUT_SECONDS = 900      # SafetyProbe: <= 15 min (it counts a small pre-load target)
COUNT_TIMEOUT_SECONDS = 3600     # CountVerify: count budget + margin (overridable)


# One dict so the README table / tests read the defaults from ONE place.
BUILT_IN_DEFAULTS = {
    "load_worker_type": LOAD_WORKER_TYPE,
    "load_num_workers": LOAD_NUM_WORKERS,
    "load_timeout_minutes": LOAD_TIMEOUT_MINUTES,
    "max_files_in_parallel": MAX_FILES_IN_PARALLEL,
    "writers_per_file": WRITERS_PER_FILE,
    "max_concurrency": MAX_CONCURRENCY,
    "discovery_worker_type": DISCOVERY_WORKER_TYPE,
    "discovery_num_workers": DISCOVERY_NUM_WORKERS,
    "discovery_timeout_minutes": DISCOVERY_TIMEOUT_MINUTES,
    "count_worker_type": COUNT_WORKER_TYPE,
    "count_num_workers": COUNT_NUM_WORKERS,
    "count_timeout_minutes": COUNT_TIMEOUT_MINUTES,
    "conn_budget": CONN_BUDGET,
    "probe_timeout_seconds": PROBE_TIMEOUT_SECONDS,
    "count_timeout_seconds": COUNT_TIMEOUT_SECONDS,
}
