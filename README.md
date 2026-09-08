# Bluegreen Observer

A small local Flask dashboard for observing and timing an externally managed MySQL blue/green switchover. It monitors a current primary and a target database, recording connection changes, read-only transitions, hostname changes, and replication-channel events.

The observer never initiates, configures, or alters a switchover. Use **Start switchover timing** when the external operation begins.

## Run locally

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python mysql_failover_timing.py
```

Open <http://127.0.0.1:5000>, provide both database addresses and MySQL credentials, then start the observer. The default sampling interval is 0.25 seconds; MySQL is contacted on port 3306 unless changed in the form.

## Recorded signals

- Initial connectivity, hostname, and read-only state for each server
- Connection loss and restoration
- Read-only and read-write changes
- Hostname changes
- Replication-channel creation, deletion, and state changes, when available

Depending on MySQL version and account privileges, replication status may not be available. Connection errors are shown in the dashboard and do not stop monitoring the other server.
