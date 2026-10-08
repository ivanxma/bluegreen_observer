#!/usr/bin/env python3
"""Run concurrent, batched InnoDB inserts against one table per worker."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class WorkerResult:
    table: str
    batches: int = 0
    rows: int = 0
    error: str = ""


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if not 0 < number < float("inf"):
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return number


def identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise argparse.ArgumentTypeError("use only letters, digits, and underscores, starting with a letter or underscore")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="MySQL server IP or hostname")
    parser.add_argument("--port", type=positive_int, default=3306)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", help="MySQL password; omit to read MYSQL_PASSWORD")
    parser.add_argument("--database", required=True, type=identifier, help="Existing database/schema")
    parser.add_argument("--threads", type=positive_int, required=True, help="Number of workers and target tables")
    parser.add_argument("--batch-size", type=positive_int, required=True, help="Rows committed in each transaction")
    parser.add_argument("--duration", type=positive_float, required=True, help="Run time in seconds")
    parser.add_argument("--row-bytes", type=positive_int, default=8192, help="Payload bytes per row (default: 8192)")
    parser.add_argument("--table-prefix", type=identifier, default="stress_txn")
    parser.add_argument("--secondary-host", help="Read-only server IP or hostname to monitor")
    parser.add_argument("--secondary-port", type=positive_int, default=3306)
    parser.add_argument("--lag-interval", type=positive_float, default=1.0, help="Seconds between lag samples")
    parser.add_argument("--catchup-timeout", type=positive_float, default=60.0,
                        help="Seconds to wait after writes for secondary catch-up")
    parser.add_argument("--lag-csv", help="Lag sample CSV path (default: mysql_lag_<run_id>.csv)")
    args = parser.parse_args(argv)
    if args.port > 65535:
        parser.error("--port must be at most 65535")
    if args.secondary_port > 65535:
        parser.error("--secondary-port must be at most 65535")
    if args.lag_csv and not args.secondary_host:
        parser.error("--lag-csv requires --secondary-host")
    if len(args.table_prefix) + len(str(args.threads)) + 1 > 64:
        parser.error("table names must fit MySQL's 64-character identifier limit")
    if args.row_bytes > 16_777_215:
        parser.error("--row-bytes must fit a MEDIUMBLOB (at most 16777215)")
    if args.password is None:
        args.password = os.environ.get("MYSQL_PASSWORD")
    if args.password is None:
        parser.error("provide --password or set MYSQL_PASSWORD")
    return args


def connect(args: argparse.Namespace, *, secondary: bool = False):
    import mysql.connector

    return mysql.connector.connect(
        host=args.secondary_host if secondary else args.host,
        port=args.secondary_port if secondary else args.port,
        user=args.user, password=args.password,
        database=args.database, connection_timeout=10, autocommit=False,
    )


def create_tables(args: argparse.Namespace, names: list[str]) -> None:
    connection = connect(args)
    try:
        cursor = connection.cursor()
        try:
            for name in names:
                cursor.execute(f"""CREATE TABLE IF NOT EXISTS `{name}` (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    run_id BINARY(16) NOT NULL,
                    batch_id BIGINT UNSIGNED NOT NULL,
                    row_in_batch INT UNSIGNED NOT NULL,
                    payload MEDIUMBLOB NOT NULL,
                    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                    KEY idx_run_batch (run_id, batch_id)
                ) ENGINE=InnoDB""")
        finally:
            cursor.close()
    finally:
        connection.close()


def write_worker(args: argparse.Namespace, name: str, run_id: bytes,
                 deadline: list[float], start: threading.Event, ready: threading.Event,
                 stop: threading.Event, committed: dict[str, int],
                 committed_lock: threading.Lock, stop_request: threading.Event | None = None,
                 on_commit: Callable[[str, int], None] | None = None) -> WorkerResult:
    result = WorkerResult(name)
    connection = None
    cursor = None
    try:
        connection = connect(args)
        cursor = connection.cursor()
        # Reuse incompressible bytes; the IDs still make each inserted row distinct.
        payload = os.urandom(args.row_bytes)
        sql = (f"INSERT INTO `{name}` (run_id, batch_id, row_in_batch, payload) "
               "VALUES (%s, %s, %s, %s)")
        ready.set()
        start.wait()
        while not stop.is_set() and not (stop_request and stop_request.is_set()) and time.monotonic() < deadline[0]:
            batch_id = result.batches + 1
            rows = [(run_id, batch_id, index, payload)
                    for index in range(args.batch_size)]
            try:
                cursor.executemany(sql, rows)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            result.batches += 1
            result.rows += args.batch_size
            with committed_lock:
                committed[name] = batch_id
            if on_commit:
                on_commit(name, batch_id)
    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        stop.set()
        ready.set()
        start.set()  # Release workers waiting if a connection fails before start.
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    return result


def read_lsn(connection) -> int:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT STORAGE_ENGINES FROM performance_schema.log_status")
        row = cursor.fetchone()
        if not row:
            raise RuntimeError("performance_schema.log_status returned no row")
        engines = json.loads(row[0]) if isinstance(row[0], (str, bytes)) else row[0]
        return int(engines["InnoDB"]["LSN"])
    finally:
        cursor.close()


def visible_batches(connection, names: list[str], run_id: bytes) -> dict[str, int]:
    cursor = connection.cursor()
    try:
        visible = {}
        for name in names:
            try:
                cursor.execute(f"SELECT COALESCE(MAX(batch_id), 0) FROM `{name}` WHERE run_id = %s", (run_id,))
                visible[name] = int(cursor.fetchone()[0])
            except Exception as exc:
                # The table DDL may still be propagating when sampling begins.
                if getattr(exc, "errno", None) != 1146:
                    raise
                visible[name] = 0
        return visible
    finally:
        cursor.close()


def monitor_lag(args: argparse.Namespace, names: list[str], run_id: bytes,
                start: threading.Event, ready: threading.Event, stop: threading.Event,
                writes_done: threading.Event, committed: dict[str, int],
                committed_lock: threading.Lock, csv_path: str,
                on_sample: Callable[[dict], None] | None = None) -> dict:
    """Sample LSNs and actual row visibility until both catch up or timeout."""
    outcome = {"error": "", "caught_up": False, "samples": 0, "max_lsn_gap": 0,
               "lsn_lag_seconds": [], "final_lsn_arrival_seconds": None,
               "final_lsn": None, "final_outstanding_batches": None,
               "csv_path": csv_path}
    primary = secondary = None
    try:
        primary = connect(args)
        secondary = connect(args, secondary=True)
        # Explicit read-only transaction handling keeps each sample fresh.
        primary.autocommit = True
        secondary.autocommit = True
        with open(csv_path, "w", newline="", encoding="utf-8") as output:
            writer = csv.writer(output)
            writer.writerow(["observed_utc", "primary_lsn", "secondary_lsn", "lsn_gap",
                             "committed_batches", "visible_batches", "outstanding_batches",
                             "oldest_pending_lsn_seconds", "newly_matched_lsn_seconds"])
            ready.set()
            start.wait()
            pending: list[tuple[int, float]] = []
            last_primary_lsn = None
            finish_deadline = None
            final_lsn = None
            final_lsn_seen_at = None
            while not stop.is_set():
                sampled_at = time.monotonic()
                primary_lsn = read_lsn(primary)
                secondary_lsn = read_lsn(secondary)
                visible = visible_batches(secondary, names, run_id)
                with committed_lock:
                    target = dict(committed)
                if last_primary_lsn is not None and primary_lsn > last_primary_lsn:
                    pending.append((primary_lsn, sampled_at))
                last_primary_lsn = primary_lsn
                matched = [round(max(0.0, sampled_at - since), 3)
                           for lsn, since in pending if secondary_lsn >= lsn]
                pending = [(lsn, since) for lsn, since in pending if secondary_lsn < lsn]
                outcome["lsn_lag_seconds"].extend(matched)
                gap = primary_lsn - secondary_lsn
                outcome["max_lsn_gap"] = max(outcome["max_lsn_gap"], max(0, gap))
                outstanding = sum(max(0, target[name] - visible[name]) for name in names)
                outcome["final_outstanding_batches"] = outstanding
                oldest = round(sampled_at - pending[0][1], 3) if pending else ""
                writer.writerow([datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                                 primary_lsn, secondary_lsn, gap, sum(target.values()),
                                 sum(visible.values()), outstanding, oldest,
                                 max(matched) if matched else ""])
                output.flush()
                outcome["samples"] += 1
                if on_sample:
                    on_sample({"primary_lsn": primary_lsn, "secondary_lsn": secondary_lsn,
                               "lsn_gap": gap, "committed_batches": sum(target.values()),
                               "visible_batches": sum(visible.values()),
                               "outstanding_batches": outstanding,
                               "oldest_pending_lsn_seconds": oldest,
                               "newly_matched_lsn_seconds": max(matched) if matched else None,
                               "samples": outcome["samples"]})
                if writes_done.is_set():
                    if finish_deadline is None:
                        finish_deadline = sampled_at + args.catchup_timeout
                        final_lsn = primary_lsn
                        final_lsn_seen_at = sampled_at
                        outcome["final_lsn"] = final_lsn
                    if secondary_lsn >= final_lsn and outcome["final_lsn_arrival_seconds"] is None:
                        outcome["final_lsn_arrival_seconds"] = round(
                            max(0, sampled_at - final_lsn_seen_at), 3)
                    if secondary_lsn >= final_lsn and outstanding == 0:
                        outcome["caught_up"] = True
                        break
                    if sampled_at >= finish_deadline:
                        break
                stop.wait(args.lag_interval)
    except Exception as exc:
        outcome["error"] = f"{type(exc).__name__}: {exc}"
        stop.set()
    finally:
        ready.set()
        start.set()
        if secondary is not None:
            secondary.close()
        if primary is not None:
            primary.close()
    return outcome


def execute(args: argparse.Namespace, stop_request: threading.Event | None = None,
            on_progress: Callable[[dict], None] | None = None) -> dict:
    def report(update: dict) -> None:
        if on_progress:
            on_progress(update)

    names = [f"{args.table_prefix}_{index}" for index in range(1, args.threads + 1)]
    report({"state": "starting", "tables": names})
    create_tables(args, names)
    run_id = uuid.uuid4().bytes
    start = threading.Event()
    stop = threading.Event()
    writes_done = threading.Event()
    committed = {name: 0 for name in names}
    committed_lock = threading.Lock()
    ready = [threading.Event() for _ in names]
    deadline = [0.0]
    lag_path = args.lag_csv or f"mysql_lag_{uuid.UUID(bytes=run_id).hex}.csv"
    lag_outcome = None
    report({"run_id": str(uuid.UUID(bytes=run_id)), "lag_csv": lag_path if args.secondary_host else None})

    def on_commit(name: str, batch_id: int) -> None:
        report({"commit": {"table": name, "batch_id": batch_id,
                           "rows": args.batch_size, "payload_bytes": args.batch_size * args.row_bytes}})

    try:
        with ThreadPoolExecutor(max_workers=args.threads + bool(args.secondary_host)) as pool:
            try:
                lag_future = None
                lag_ready = threading.Event()
                if args.secondary_host:
                    lag_future = pool.submit(monitor_lag, args, names, run_id, start, lag_ready,
                                             stop, writes_done, committed, committed_lock, lag_path,
                                             lambda sample: report({"sample": sample}))
                futures = [pool.submit(write_worker, args, name, run_id, deadline, start, signal,
                                       stop, committed, committed_lock, stop_request, on_commit)
                           for name, signal in zip(names, ready)]
                for signal in ready:
                    signal.wait()
                if lag_future:
                    lag_ready.wait()
                started_at = time.monotonic()
                deadline[0] = started_at + args.duration
                start.set()
                report({"state": "running", "started_at": datetime.now(timezone.utc).isoformat(),
                        "write_deadline": deadline[0]})
                results = [future.result() for future in futures]
                write_elapsed = time.monotonic() - started_at
                writes_done.set()
                report({"state": "catching_up" if lag_future else "finishing"})
                if lag_future:
                    lag_outcome = lag_future.result()
            except BaseException:
                stop.set()
                start.set()
                writes_done.set()
                raise
    except KeyboardInterrupt:
        stop.set()
        start.set()
        writes_done.set()
        raise
    total_batches = sum(item.batches for item in results)
    total_rows = sum(item.rows for item in results)
    lags = lag_outcome["lsn_lag_seconds"] if lag_outcome else []
    failed = any(item.error for item in results) or bool(
        lag_outcome and (lag_outcome["error"] or not lag_outcome["caught_up"]))
    return {
        "state": "failed" if failed else "completed",
        "run_id": str(uuid.UUID(bytes=run_id)),
        "stop_reason": "manual" if stop_request and stop_request.is_set() else "duration",
        "write_seconds": round(write_elapsed, 3),
        "transactions": total_batches, "rows": total_rows,
        "rows_per_commit": args.batch_size,
        "payload_bytes_per_row": args.row_bytes,
        "payload_bytes_per_commit": args.batch_size * args.row_bytes,
        "total_payload_bytes": total_rows * args.row_bytes,
        "rows_per_second": round(total_rows / write_elapsed, 1),
        "tables": [vars(item) for item in results],
        "secondary": None if not lag_outcome else {
            "caught_up": lag_outcome["caught_up"],
            "samples": lag_outcome["samples"],
            "max_sampled_lsn_gap": lag_outcome["max_lsn_gap"],
            "lsn_arrivals": len(lags),
            "mean_lsn_arrival_seconds": round(sum(lags) / len(lags), 3) if lags else None,
            "max_lsn_arrival_seconds": max(lags) if lags else None,
            "final_primary_lsn": lag_outcome["final_lsn"],
            "final_lsn_arrival_seconds": lag_outcome["final_lsn_arrival_seconds"],
            "final_outstanding_batches": lag_outcome["final_outstanding_batches"],
            "lag_csv": lag_outcome["csv_path"],
            "error": lag_outcome["error"],
        },
    }


def run(args: argparse.Namespace) -> int:
    result = execute(args)
    for table in result["tables"]:
        print(f"{table['table']}: {table['batches']} transactions, {table['rows']} rows"
              + (f", ERROR: {table['error']}" if table["error"] else ""))
    print(f"Total: {result['transactions']} transactions, {result['rows']} rows in "
          f"{result['write_seconds']:.2f}s ({result['rows_per_second']:.1f} rows/s); "
          f"commit: {result['rows_per_commit']} rows, "
          f"{result['payload_bytes_per_commit']} payload bytes; run ID: {result['run_id']}")
    if result["secondary"]:
        secondary = result["secondary"]
        print(f"Secondary: {'caught up' if secondary['caught_up'] else 'did not catch up'}; "
              f"{secondary['samples']} samples; max sampled LSN gap "
              f"{secondary['max_sampled_lsn_gap']}; max observed LSN arrival "
              f"{secondary['max_lsn_arrival_seconds']}s; lag CSV: {secondary['lag_csv']}")
        if secondary["error"]:
            print(f"Lag monitor ERROR: {secondary['error']}", file=sys.stderr)
    return 1 if result["state"] == "failed" else 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        print("Interrupted; current transactions have finished.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Stress test error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
