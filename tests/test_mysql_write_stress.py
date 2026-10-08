import contextlib
import io
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import mysql_write_stress as stress


class FakeCursor:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []
        self.closed = False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def executemany(self, sql, rows):
        self.calls.append((sql, rows))
        if self.fail:
            raise RuntimeError("write failed")

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, fail=False):
        self.cur = FakeCursor(fail)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.autocommit = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class StressTests(unittest.TestCase):
    def args(self, *extra):
        return stress.parse_args(["--host", "primary", "--user", "u", "--password", "p",
                                  "--database", "testdb", "--threads", "2",
                                  "--batch-size", "3", "--duration", "1", *extra])

    def test_creates_one_innodb_table_per_thread(self):
        connection = FakeConnection()
        with patch.object(stress, "connect", return_value=connection):
            stress.create_tables(self.args(), ["stress_txn_1", "stress_txn_2"])
        self.assertEqual(len(connection.cur.calls), 2)
        self.assertIn("`stress_txn_1`", connection.cur.calls[0][0])
        self.assertIn("ENGINE=InnoDB", connection.cur.calls[1][0])
        self.assertTrue(connection.closed)

    def test_commits_whole_batch_and_reports_rows(self):
        connection = FakeConnection()
        start, ready, stop = threading.Event(), threading.Event(), threading.Event()
        start.set()
        with patch.object(stress, "connect", return_value=connection), \
             patch.object(stress.time, "monotonic", side_effect=[0, 2]):
            result = stress.write_worker(self.args(), "stress_txn_1", b"a" * 16,
                                         [1], start, ready, stop, {}, threading.Lock())
        self.assertEqual((result.batches, result.rows, result.error), (1, 3, ""))
        self.assertEqual(connection.commits, 1)
        self.assertEqual(len(connection.cur.calls[0][1]), 3)
        self.assertTrue(connection.closed)

    def test_rolls_back_failed_batch(self):
        connection = FakeConnection(fail=True)
        start, ready, stop = threading.Event(), threading.Event(), threading.Event()
        start.set()
        with patch.object(stress, "connect", return_value=connection), \
             patch.object(stress.time, "monotonic", return_value=0):
            result = stress.write_worker(self.args(), "stress_txn_1", b"a" * 16,
                                         [1], start, ready, stop, {}, threading.Lock())
        self.assertEqual(result.rows, 0)
        self.assertEqual(connection.rollbacks, 1)
        self.assertTrue(stop.is_set())

    def test_secondary_catchup_requires_lsn_and_visible_batch(self):
        primary, secondary = FakeConnection(), FakeConnection()
        start, ready, stop, done = (threading.Event() for _ in range(4))
        start.set()
        done.set()
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(stress, "connect", side_effect=[primary, secondary]), \
             patch.object(stress, "read_lsn", side_effect=[100, 90, 100, 100]), \
             patch.object(stress, "visible_batches", side_effect=[{"t": 0}, {"t": 1}]):
            csv_path = str(Path(directory) / "lag.csv")
            outcome = stress.monitor_lag(self.args("--secondary-host", "secondary",
                                                   "--lag-interval", "0.001"),
                                         ["t"], b"a" * 16, start, ready, stop, done,
                                         {"t": 1}, threading.Lock(), csv_path)
            self.assertEqual(len(Path(csv_path).read_text().splitlines()), 3)
        self.assertTrue(outcome["caught_up"])
        self.assertEqual(outcome["max_lsn_gap"], 10)
        self.assertTrue(primary.closed and secondary.closed)

    def test_row_and_payload_normalization(self):
        metrics = stress.normalized_workload(2.0, 1000, 1024)
        self.assertEqual(metrics["ms_per_1000_rows"], 2000)
        self.assertEqual(metrics["ms_per_mib_payload"], 2048)
        self.assertEqual(metrics["rows_per_second"], 500)
        self.assertEqual(metrics["payload_mib_per_second"], 0.488)
        self.assertIsNone(stress.normalized_workload(2.0, 0, 1024)["ms_per_1000_rows"])

    def test_lsn_milestone_records_row_and_payload_rates(self):
        primary, secondary = FakeConnection(), FakeConnection()
        start, ready, stop, done = (threading.Event() for _ in range(4))
        start.set()

        def after_sample(sample):
            if sample["samples"] == 2:
                done.set()

        target_lsn = 100 + 1024 * 1024
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(stress, "connect", side_effect=[primary, secondary]), \
             patch.object(stress, "read_lsn", side_effect=[100, 90, target_lsn, 100,
                                                            target_lsn, target_lsn]), \
             patch.object(stress, "visible_batches", side_effect=[{"t": 0}, {"t": 0}, {"t": 1}]):
            csv_path = str(Path(directory) / "lag.csv")
            outcome = stress.monitor_lag(self.args("--secondary-host", "secondary",
                                                   "--lag-interval", "0.001"),
                                         ["t"], b"a" * 16, start, ready, stop, done,
                                         {"t": 1}, threading.Lock(), csv_path, after_sample)
            csv_text = Path(csv_path).read_text()
        self.assertTrue(outcome["caught_up"])
        self.assertEqual(len(outcome["workload_ms_per_1000_rows"]), 1)
        self.assertEqual(len(outcome["workload_ms_per_mib_payload"]), 1)
        self.assertIsNotNone(outcome["final_workload"]["rows_per_second"])
        self.assertIn("newly_matched_ms_per_1000_rows", csv_text)
        self.assertIn("newly_matched_ms_per_mib_payload", csv_text)

    def test_invalid_identifiers_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.args("--table-prefix", "bad`name")

    def test_execute_summarizes_manual_and_duration_stop(self):
        def fake_worker(args, name, run_id, deadline, start, ready, stop,
                        committed, committed_lock, stop_request, on_commit):
            ready.set()
            start.wait(1)
            on_commit(name, 1)
            return stress.WorkerResult(name, 1, args.batch_size)

        config = self.args()
        config.secondary_host = None
        updates = []
        with patch.object(stress, "create_tables"), patch.object(stress, "write_worker", side_effect=fake_worker):
            automatic = stress.execute(config, on_progress=updates.append)
            manual_event = threading.Event()
            manual_event.set()
            manual = stress.execute(config, stop_request=manual_event)
        self.assertEqual(automatic["stop_reason"], "duration")
        self.assertEqual(manual["stop_reason"], "manual")
        self.assertEqual(automatic["rows"], 6)
        self.assertEqual(automatic["transactions"], 2)
        self.assertEqual(automatic["payload_bytes_per_commit"], 3 * 8192)
        self.assertEqual(len([u for u in updates if "commit" in u]), 2)


if __name__ == "__main__":
    unittest.main()
