import unittest
from unittest.mock import patch

import elasticdb_cluster_observer as observer


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, sql):
        if self.connection.broken:
            raise ConnectionError("connection lost")
        self.sql = sql
        self.connection.queries.append(sql)
        if self.connection.deny_variables and "GLOBAL VARIABLES" in sql:
            raise PermissionError("variables unavailable")

    def fetchone(self):
        if "@@hostname" in self.sql:
            return ("db-1", 0, 0)
        if "gtid_executed" in self.sql:
            return ("",)
        if "log_status" in self.sql:
            return ("uuid-1", '{"InnoDB":{"LSN":100,"LSN_checkpoint":90}}')
        if "Uptime" in self.sql:
            return ("Uptime", "120")
        if "CONNECTION_ID" in self.sql:
            return (self.connection.id,)
        return (1,)

    def fetchall(self):
        if "GLOBAL STATUS LIKE '%airglow%'" in self.sql:
            return [("Airglow_ready", "ON"), ("Airglow_counter", "0")]
        if "GLOBAL VARIABLES LIKE '%airglow%'" in self.sql:
            return [("airglow_enabled", "ON")]
        return []


class Connection:
    def __init__(self, id=1):
        self.id = id
        self.broken = False
        self.closed = False
        self.queries = []
        self.deny_variables = False

    def cursor(self):
        return Cursor(self)

    def close(self):
        self.closed = True


class ObserverTests(unittest.TestCase):
    def setUp(self):
        self.monitor = observer.ElasticDBClusterMonitor({
            "servers": [{"name": "DB 1", "host": "db1", "port": 3306}],
            "user": "reader", "password": "secret", "tables": [], "interval": 0.1,
        })

    def test_cached_connection_reused_and_health_checked(self):
        connection = Connection()
        with patch.object(observer, "connect", return_value=connection) as connect:
            first = self.monitor.query_cached(self.monitor.cached_servers[0])
            second = self.monitor.query_cached(first)
        self.assertEqual(connect.call_count, 1)
        self.assertTrue(second.connected)
        self.assertEqual(second.uptime_seconds, 120)
        self.assertEqual(connection.queries.count("SELECT 1"), 2)
        self.assertFalse(connection.closed)
        self.assertFalse(any("airglow" in sql.lower() for sql in connection.queries))

    def test_outage_retries_duration_and_history(self):
        first_connection, replacement = Connection(), Connection(2)
        with patch.object(observer, "connect", side_effect=[first_connection, ConnectionError("offline"), replacement]):
            initial = self.monitor.query_cached(self.monitor.cached_servers[0])
            first_connection.broken = True
            with patch.object(observer.time, "monotonic", return_value=10), patch.object(observer, "now", return_value="outage UTC"):
                lost = self.monitor.query_cached(initial)
            with patch.object(observer.time, "monotonic", return_value=14):
                retry = self.monitor.query_cached(lost)
            with patch.object(observer.time, "monotonic", return_value=18), patch.object(observer, "now", return_value="restored UTC"):
                restored = self.monitor.query_cached(retry)
            later = self.monitor.query_cached(restored)
        self.assertTrue(first_connection.closed)
        self.assertFalse(lost.connected)
        self.assertIsNone(lost.uptime_seconds)
        self.assertEqual(retry.last_outage_at, "outage UTC")
        self.assertEqual(retry.outage_duration_seconds, 4)
        self.assertEqual(restored.outage_duration_seconds, 8)
        self.assertEqual(restored.last_restored_at, "restored UTC")
        self.assertFalse(restored.outage_active)
        self.assertEqual(later.last_outage_at, "outage UTC")
        self.assertEqual(later.outage_duration_seconds, 8)
        # Published samples must not be mutated by later polls.
        self.assertTrue(initial.connected)
        self.assertFalse(initial.outage_active)

    def test_initial_failure_is_an_outage(self):
        with patch.object(observer, "connect", side_effect=ConnectionError("offline")):
            sample = self.monitor.query_cached(self.monitor.cached_servers[0])
        self.assertTrue(sample.outage_active)
        self.assertEqual(sample.status, "Reconnecting")
        self.assertTrue(sample.last_outage_at)

    def test_optional_airglow_error_keeps_fresh_status_available(self):
        connection = Connection()
        connection.deny_variables = True
        with patch("mysql_multi_observer.connect", return_value=connection):
            sample = self.monitor.query(self.monitor.servers[0])
        self.assertTrue(sample.connected)
        self.assertIn("variables", sample.airglow_errors)
        self.assertEqual(sample.airglow["status"]["Airglow_ready"], "ON")

    def test_fresh_samples_open_and_close_connections_and_collect_airglow(self):
        import mysql_multi_observer
        connections = [Connection(), Connection(2)]
        with patch.object(mysql_multi_observer, "connect", side_effect=connections) as connect:
            first = self.monitor.query(self.monitor.servers[0])
            second = self.monitor.query(first)
        self.assertEqual(connect.call_count, 2)
        self.assertTrue(all(c.closed for c in connections))
        self.assertTrue(second.connected)
        self.assertEqual(second.airglow["variables"], {"airglow_enabled": "ON"})

    def test_run_stop_closes_cached_connections(self):
        connection = Connection()
        original = self.monitor.query_cached

        def stop_after_sample(old):
            sample = original(old)
            self.monitor.stop_event.set()
            return sample

        with patch.object(observer, "connect", return_value=connection), patch.object(self.monitor, "query_cached", side_effect=stop_after_sample), patch.object(self.monitor, "query", side_effect=lambda s: s):
            self.monitor.run()
        self.assertTrue(connection.closed)
        self.assertEqual(self.monitor.connections, {})
        self.assertEqual(self.monitor.cached_servers[0].status, "Stopped")

    def test_routes_allow_status_without_selected_tables_and_hide_credentials(self):
        with patch.object(observer, "monitor", None), patch.object(observer.ElasticDBClusterMonitor, "start"):
            client = observer.app.test_client()
            self.assertEqual(client.get("/api/status").json["cached_servers"], [])
            response = client.post("/api/start", json=self.monitor.config)
            self.assertEqual(response.status_code, 200)
            status = client.get("/api/status")
            self.assertNotIn("secret", status.get_data(as_text=True))
            self.assertIn("cached_servers", status.json)
            page = client.get("/").get_data(as_text=True)
            self.assertLess(page.index('id="servers"'), page.index('id="cachedServers"'))
            self.assertNotIn('class="left"', page)
            self.assertEqual(client.post("/api/stop").status_code, 200)
            self.assertEqual(client.post("/api/start", json={**self.monitor.config, "tables": ["bad`name"]}).status_code, 400)


if __name__ == "__main__":
    unittest.main()
