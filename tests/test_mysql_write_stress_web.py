import threading
import unittest
from unittest.mock import patch

import mysql_write_stress_web as web


def valid_config():
    return {"host": "primary", "secondary_host": "secondary", "database": "loadtest",
            "user": "writer", "password": "secret", "threads": "2", "batch_size": "10",
            "duration": "30", "row_bytes": "8192"}


class WebTests(unittest.TestCase):
    def setUp(self):
        self.controller = web.RunController()
        self.patch = patch.object(web, "controller", self.controller)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.client = web.app.test_client()

    def test_page_and_config_validation(self):
        page = self.client.get("/")
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"Secondary IP or host", page.data)
        self.assertIn(b"Stop writes", page.data)
        self.assertIn(b"Normalized by pending rows", page.data)
        self.assertIn(b"Normalized by pending payload", page.data)
        self.assertEqual(self.client.post("/api/start", json={"password": "secret"}).status_code, 400)
        config = web.config_from_json(valid_config())
        self.assertEqual((config.threads, config.batch_size, config.row_bytes), (2, 10, 8192))
        self.assertEqual(config.secondary_host, "secondary")
        spaced = {**valid_config(), "password": " secret "}
        self.assertEqual(web.config_from_json(spaced).password, " secret ")
        self.assertEqual(self.client.get("/api/status").json["state"], "idle")

    def test_start_stop_returns_summary_without_password(self):
        entered = threading.Event()
        release = threading.Event()

        def fake_execute(args, stop_request, progress):
            progress({"state": "running", "run_id": "run-1", "tables": ["stress_txn_1"]})
            progress({"commit": {"table": "stress_txn_1", "batch_id": 1,
                                "rows": args.batch_size, "payload_bytes": args.batch_size * args.row_bytes}})
            entered.set()
            release.wait(2)
            return {"state": "completed", "stop_reason": "manual", "transactions": 1,
                    "rows": args.batch_size, "total_payload_bytes": args.batch_size * args.row_bytes,
                    "tables": [], "secondary": {"final_lsn_arrival_seconds": 0.5}}

        with patch.object(web, "execute", side_effect=fake_execute):
            self.assertEqual(self.client.post("/api/start", json=valid_config()).status_code, 202)
            self.assertTrue(entered.wait(1))
            live = self.client.get("/api/status").json
            self.assertEqual(live["rows"], 10)
            self.assertNotIn("secret", str(live))
            self.assertEqual(self.client.post("/api/start", json=valid_config()).status_code, 409)
            self.assertEqual(self.client.post("/api/stop").status_code, 202)
            self.assertTrue(self.controller.stop_request.is_set())
            release.set()
            self.controller.thread.join(2)
            final = self.client.get("/api/status").json
            self.assertEqual(final["state"], "completed")
            self.assertEqual(final["summary"]["secondary"]["final_lsn_arrival_seconds"], 0.5)
            self.assertEqual(self.client.post("/api/stop").status_code, 409)

    def test_unsafe_batch_rejected(self):
        config = valid_config()
        config.update(batch_size="100000", row_bytes="8192")
        response = self.client.post("/api/start", json=config)
        self.assertEqual(response.status_code, 400)
        self.assertIn("64 MiB", response.json["error"])


if __name__ == "__main__":
    unittest.main()
