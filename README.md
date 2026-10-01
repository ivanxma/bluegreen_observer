# Bluegreen Observer

Local Flask dashboards for watching MySQL servers during an externally managed blue/green switchover. Both dashboards only query MySQL; neither initiates or changes a switchover.

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

The left panel shows each server's connection state, hostname, read-only state, uptime, replication channels, and `@@GLOBAL.gtid_executed` value. The right panel shows timestamped connection, hostname, read-only, GTID, replication, table-content, and uptime reset events above the selected table content. Each server's status and table content share a distinct color. Routine uptime increments do not create events. Each table displays up to 50 sampled rows. The account needs permission to read the selected tables; table errors appear in the dashboard without stopping observation of the other servers.
