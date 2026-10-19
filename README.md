# Bluegreen Observer

## Blue Green administration console

The [MyApp web application](myapp/README.md) provides instance-principal OCI automation, a combined Blue/Green dashboard, HA conversion, Vault-backed credentials, GTID checks, work-request history, and scheduled-event review/restoration for creation and switchover.


Local Flask dashboards for watching MySQL servers during an externally managed blue/green switchover, plus a separate write stress tool. The observer dashboards only query MySQL; none initiates or changes a switchover.

## Install

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

## Two-server switchover timer

Run `python mysql_failover_timing.py` and open <http://127.0.0.1:5050>. Enter the current primary and target addresses and MySQL credentials, then start the observer. Click **Start switchover timing** when the external operation begins to measure subsequent events. The default sample interval is 0.25 seconds, and the default MySQL port is 3306.

The timer records:

- Initial connectivity, hostname, and read-only state for each server
- Connection loss and restoration
- Read-only and read-write changes
- Hostname changes
- Replication-channel creation, deletion, and state changes, when available

Depending on MySQL version and account privileges, replication status may not be available. Connection errors appear in the dashboard.

## Multi-server observer

Run `python mysql_multi_observer.py` and open <http://127.0.0.1:5051>. Add 1–16 distinct DB servers, enter shared MySQL credentials, and choose **Load tables** to list `performance_schema` tables from the first reachable server. Select 1–8 tables, then click **Start observe**. The default sample interval is one second; each server can use its own MySQL port.

The left panel shows each server's connection state, hostname, read-only state, uptime, replication channels, `@@GLOBAL.gtid_executed` value, server UUID, InnoDB LSN, and InnoDB `LSN_checkpoint`. The UUID and both LSN values come from `performance_schema.log_status`; if that query fails, the card displays the error. The right panel shows timestamped connection, hostname, read-only, GTID, replication, table-content, and uptime reset events above the selected table content. Each server's status and table content share a distinct color. Routine uptime increments and LSN updates do not create events. Table reads establish an initial sample; later reads create a table-content event only when sampled columns or values change, regardless of row order. Each table displays up to 50 sampled rows. The account needs permission to read the selected tables; table errors appear in the dashboard without stopping observation of the other servers.

Reading `performance_schema.log_status` requires `SELECT` and `BACKUP_ADMIN`. Each read briefly pauses logging while MySQL collects a consistent snapshot, so choose the sample interval with that overhead in mind.

The **LSN propagation** table treats the first configured server as primary. When its sampled InnoDB LSN increases, the table adds a row for each other server and measures the time until that server's sampled LSN reaches or exceeds the primary value. It shows both servers' LSN and `LSN_checkpoint` values. This measurement assumes the servers share comparable redo LSNs, as in this deployment; its precision is limited by the sample interval.


## ElasticDBCluster Observer

Run `python elasticdb_cluster_observer.py` and open <http://127.0.0.1:5052>.
Add 1–16 distinct servers and shared MySQL credentials. Selected
`performance_schema` tables are optional (up to eight). Use `--port` to change
the dashboard port.

The first table has one row per attribute and one column per server. Each sample
opens a new connection to each server and closes it after reading status and
selected table content. It includes the multi-server observer's attributes plus
all values returned by `SHOW GLOBAL STATUS LIKE '%airglow%'` and
`SHOW GLOBAL VARIABLES LIKE '%airglow%'`. Matching names across servers are
combined into rows labelled **Status** or **Variable**. Query errors and missing
attributes appear in the relevant server column.

Directly below, the cached connection table shows only uptime, connection status,
last outage time, outage duration, last restoration time, and the outage event.
It retains one connection per server across samples and checks it with `SELECT 1`
before reading uptime. On failure it closes the connection and retries on each
following sample until successful. Stopping closes all cached connections; wait
for shutdown to finish before starting again.

Outages start at the first failed check (including an initial connection failure)
and end at the first successful reconnection. Times use UTC; durations use a
monotonic clock. An ongoing duration is updated at each check, and the most recent
outage remains visible after recovery. Detection precision is limited by the
sample interval and query timeouts. Uptime means server uptime; it is unavailable
while the cached connection is down. Events, selected table content, and LSN
propagation are retained below the two status tables.

Observer and test-case inputs save automatically in the same browser, including
server rows, username, interval, database, threads, sleep timer,
loop pauses, table choices and active tab. **Save settings** also saves immediately.
Values return when the dashboard is reopened at the same address, including
after an app restart. Passwords are excluded and must be entered again. Restoring
settings does not start observation or workloads. Browser storage must be enabled.

The dashboard has **Observer**, **Test 1 · INSERT**, **Test 2 · SLEEP** and
**Test 3 · DDL + consistency** tabs. Switching tabs keeps all running tests and
observation active. Configure the Primary endpoint and Node 1 / Node 2 in the
Observer server list and click **Start observe** to apply the shared endpoints
and credentials. Tests have no separate IP or port fields. **Detect server roles**
queries `read_only` and `super_read_only`; a node is read-only if either is set.
Aliases of the same server UUID are deduplicated. Writing is rejected when
multiple different writable servers are detected. Each test captures the applied
Observer configuration at Start; reapply changed endpoints before starting a new
run. Stopping a test does not stop observation; stopping observation does not stop
the tests.

- **Test 1 — Primary INSERT load:** specify threads (1–64),
  existing database, loop pause and batch size (1–10000 rows per commit).
  Choose 1–64 data columns and set each column's name and `VARCHAR(x)` width
  (1–10000 characters, subject to the combined row-size limit). Each run creates
  a new `elastic_insert_<run>` InnoDB table with an auto-increment `id`,
  `createdby VARCHAR(255)` and those data columns. Every INSERT supplies
  `createdby = @@hostname`; each data value is generated by MySQL with
  `REPEAT('x', column_width)`. Workers insert the requested rows and commit once
  per batch, looping until Stop. The account needs CREATE, SELECT and INSERT.
  The table and inserted data remain after Stop. The UI previews the schema and
  INSERT, and reports confirmed committed rows/batches and average rows/second.
  Column definitions and batch size are saved with the other settings. Estimated
  generated data is limited to 16 MiB per batch and 64 MiB across workers.
- **Test 2 — Read-only long queries:** specify threads, sleep timer
  (0.1–3600 seconds) and loop pause. Each worker reuses its
  session for repeated `SELECT SLEEP(...)`. Stop attempts to interrupt active
  queries using a separate same-account connection on the same backend. If
  cancellation cannot reach that backend, the query finishes or times out before
  the worker stops. Primary transactions finish their current attempt on Stop.
- **Test 3 — DDL + read-node consistency:** specify an existing database,
  threads, rows per batch, batches per cycle, payload VARCHAR width, loop pause
  and a reader convergence timeout. Each worker owns an `elastic_ddl_<run>_<worker>`
  table and repeats: DROP any prior interrupted cycle, confirm reader absence,
  CREATE, INSERT/COMMIT batches, compare primary and read-only schema/data, DROP,
  then confirm reader absence. Rows include explicit IDs, `createdby = @@hostname`,
  a unique cycle token and `REPEAT('x', width)` payload. Checks compare column
  definitions, row count, complete ID range, payload, cycle token and hostname on
  every detected read-only node. The UI shows per-node results, elapsed check
  time, DDL counts, committed rows/batches and convergence timeouts. A missing or
  unreachable Observer endpoint prevents a cycle from being marked consistent.
  Requires CREATE, DROP, INSERT and SELECT. Only this run's tables are dropped;
  Stop or an error can leave an incomplete cycle's table for inspection.

Role discovery refreshes when opening sessions and while checking readers.
Test 1 writes to the detected primary. Test 2 distributes workers over detected
read-only servers. Persistent sessions check their role between operations;
following a role change or connection failure they reconnect using the same
Observer endpoint list. Test 3 checks the current read-only set while waiting for
convergence. A timeout reports a failure to converge within the configured time;
it does not establish permanent data corruption.

Each test also opens a fresh login and runs SELECT 1 every second independently
of its persistent workload. Per-session results include the backend hostname,
connection ID, completed operations, failures, connections, reconnects, last/max
operation latency, last/max interruption, UTC success/failure times and errors.
UTC events retain the latest 500 entries per test. Interrupted COMMIT replies
are counted as uncertain commits because the server may have committed even
though the client did not receive confirmation. Interruption is measured from
the start of a failed operation until a subsequent successful operation and
includes query and retry delays; it is sampled client impact rather than an
exact server outage. Workload connections retry after failures and continue
until Stop. Test state and results are held in memory and reset on app restart.

Run the test suite with:

```bash
python3 -m unittest discover -s tests -v
```

## MySQL write stress and secondary lag

Start the web interface after installing the requirements:

```bash
python3 mysql_write_stress_web.py
```

Open <http://127.0.0.1:5053>. Enter the primary and secondary IP addresses and ports,
the shared MySQL user and password, an existing database, thread count, rows per
commit, payload size, and duration. **Start test** creates one table per worker
and begins writing. **Stop writes** lets current transactions finish and then
waits for the secondary to catch up (or for the configured timeout). The live
status and final result use tables with groups for workload, secondary catch-up,
raw LSN timing, normalization by rows, and normalization by payload size. The
result includes median and final values for both normalized latency and observed
catch-up rate. The page offers a CSV download with all lag samples. The web
server binds to localhost by default; use `--host` and `--port` to change its
listening address and port.

The workload creates one InnoDB table per worker in the existing database and
commits each batch as one transaction. Rows have an 8 KiB binary payload by
default. Existing rows are retained. The account needs `CREATE` and `INSERT`
on the primary database. The web interface accepts up to 64 threads and limits
each batch to 64 MiB of payload.

The same workload is available from the CLI:

```bash
export MYSQL_PASSWORD='your-password'
python3 mysql_write_stress.py --host 10.0.0.1 --port 3306 \
  --secondary-host 10.0.0.2 --secondary-port 3306 \
  --user stress --database loadtest --threads 8 --batch-size 100 \
  --row-bytes 8192 --duration 60
```

`--secondary-host` is optional for the CLI and required in the web interface.
With it, the program reads each target table on
the secondary and samples both servers' InnoDB LSN from
`performance_schema.log_status` every second. It writes timestamped LSN values,
their signed gap, and committed versus visible batch counts to
`mysql_lag_<run_id>.csv`. Use `--lag-csv` to choose another path,
`--lag-interval` to change the sample period, and `--catchup-timeout` to set how
long to wait after writes stop (default 60 seconds). The secondary account needs
`SELECT` on the target tables and `SELECT` plus `BACKUP_ADMIN` for `log_status`.
The tool exits nonzero if a worker fails or the secondary does not catch up.

LSN comparisons represent propagation time only when the two servers use
comparable InnoDB redo LSNs. Ordinary MySQL replicas can have independent LSN
sequences; in that case use the CSV's visible batch counts as the replication
signal. LSN arrival latency starts when a primary LSN is sampled and ends when
the secondary is first sampled at or above it; its precision is limited by
`--lag-interval`. For each primary LSN milestone, the program counts test rows
committed but not yet visible on the secondary at that sample. It divides the
milestone's arrival time by those pending rows (reported as ms/1,000 rows) and
by their payload size (reported as ms/MiB). It also reports the inverse rates in
rows/s and MiB/s. The final result shows medians across matched milestones and
the final LSN's values. Samples with no pending test rows or no positive LSN gap
have no row or payload normalized value. These measures make different batch
sizes easier to compare, but queueing, concurrent writes, and the sample
interval still affect them. The
CSV also includes normalization by the sampled redo LSN gap. The displayed
commit size counts payload bytes, excluding row and index overhead. Keep
`--batch-size` × `--row-bytes` below the server's
`max_allowed_packet`, since the connector may send a batch as one multi-row
statement. Stop the CLI with Ctrl+C.
