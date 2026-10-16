import threading
import unittest
from unittest.mock import patch

import elasticdb_cluster_observer as observer
import elasticdb_test_client as workload


def config(kind='primary', **overrides):
    return workload.config_from_json(dict(host='vip', port=3306, user='tester', password='secret',
        database='testdb', threads=1, sleep=20, pause=.01, **overrides), kind)


class Cursor:
    def __init__(self, con):
        self.con = con
        self.sql = ''

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.con.queries.append((sql, params))
        if sql.startswith('SELECT SLEEP'):
            self.con.sleeping.set()
            self.con.release.wait(2)
            if self.con.killed:
                raise ConnectionError('Query interrupted')
        if sql.startswith('KILL QUERY'):
            self.con.target.killed = True
            self.con.target.release.set()
        if self.con.fail_update and sql.startswith('UPDATE'):
            self.con.run.stop_event.set()
            raise ConnectionError('write failed')

    def fetchone(self):
        if 'CONNECTION_ID()' in self.sql:
            return ('backend', self.con.id, self.con.uuid)
        if 'server_uuid' in self.sql:
            return (self.con.uuid,)
        return (0,)


class Connection:
    def __init__(self, run=None, id=1, uuid='uuid-1'):
        self.run, self.id, self.uuid = run, id, uuid
        self.queries = []
        self.closed = self.killed = self.fail_update = False
        self.fail_commit = False
        self.commits = self.rollbacks = self.transactions = 0
        self.sleeping, self.release = threading.Event(), threading.Event()
        self.target = None

    def cursor(self):
        return Cursor(self)

    def start_transaction(self):
        self.transactions += 1

    def commit(self):
        self.commits += 1
        if self.fail_commit:
            raise ConnectionError('commit response lost')
        self.run.stop_event.set()

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class WorkloadTests(unittest.TestCase):
    def test_primary_transaction_and_cleanup(self):
        run = workload.TestRun('primary', config())
        con = Connection(run)
        with patch.object(workload, 'open_connection', return_value=con):
            run.worker('worker-1')
        statements = [sql for sql, _ in con.queries]
        self.assertTrue(any(s.startswith('CREATE TABLE IF NOT EXISTS `testdb`') for s in statements))
        for prefix in ('INSERT', 'UPDATE', 'SELECT counter'):
            self.assertTrue(any(s.startswith(prefix) for s in statements))
        self.assertEqual((con.transactions, con.commits), (1, 1))
        self.assertEqual(run.snapshot()['sessions']['worker-1']['successes'], 1)
        self.assertTrue(con.closed)
        self.assertFalse(run.active)

    def test_failed_transaction_rolls_back(self):
        run = workload.TestRun('primary', config())
        con = Connection(run)
        con.fail_update = True
        with patch.object(workload, 'open_connection', return_value=con):
            run.worker('worker-1')
        self.assertEqual((con.commits, con.rollbacks), (0, 1))
        self.assertTrue(con.closed)

    def test_ambiguous_commit_recorded_then_new_connection(self):
        run = workload.TestRun('primary', config())
        bad, good = Connection(run), Connection(run, 2)
        bad.fail_commit = True
        with patch.object(workload, 'open_connection', side_effect=[bad, good]):
            run.worker('worker-1')
        s = run.snapshot()['sessions']['worker-1']
        self.assertEqual((s['failures'], s['uncertain_commits'], s['successes'], s['connections']), (1, 1, 1, 2))
        self.assertFalse(s['outage_active'])
        self.assertEqual(bad.rollbacks, 1)
        self.assertTrue(bad.closed and good.closed)
        self.assertIn('recovered', [e['event'] for e in run.events])

    def test_sleep_stop_interrupts_and_closes_without_outage(self):
        run = workload.TestRun('readonly', config('readonly'))
        con, control = Connection(run), Connection(run, 2)
        control.target = con
        with patch.object(workload, 'open_connection', side_effect=[con, control]):
            thread = threading.Thread(target=run.worker, args=('worker-1',))
            thread.start()
            self.assertTrue(con.sleeping.wait(1))
            run.stop_event.set()
            run.cancel_queries()
            thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertTrue(con.killed and con.closed and control.closed)
        self.assertEqual(run.snapshot()['sessions']['worker-1']['failures'], 0)
        self.assertIn(('SELECT SLEEP(%s)', (20.0,)), con.queries)

    def test_stop_never_kills_connection_on_different_backend(self):
        run = workload.TestRun('readonly', config('readonly'))
        run.active['worker-1'] = (123, 'old-backend')
        control = Connection(run, uuid='new-backend')
        with patch.object(workload, 'open_connection', return_value=control):
            run.cancel_queries()
        self.assertFalse(any(sql.startswith('KILL') for sql, _ in control.queries))
        self.assertIn('different backend', run.cancel_error)
        self.assertTrue(control.closed)

    def test_fresh_probe_closes_each_login(self):
        run = workload.TestRun('readonly', config('readonly'))
        connections = [Connection(run), Connection(run, 2)]
        count = 0
        def wait(_):
            nonlocal count
            count += 1
            if count == 2:
                run.stop_event.set()
        with patch.object(workload, 'open_connection', side_effect=connections), patch.object(run.stop_event, 'wait', side_effect=wait):
            run.worker('fresh-login')
        self.assertTrue(all(c.closed for c in connections))
        self.assertEqual(run.snapshot()['sessions']['fresh-login']['successes'], 2)
        self.assertFalse(any('SLEEP' in sql for c in connections for sql, _ in c.queries))

    def test_outage_duration_and_redaction(self):
        run = workload.TestRun('primary', config())
        with patch.object(workload.time, 'monotonic', return_value=12):
            run.record('worker-1', 10, ConnectionError('secret'))
        with patch.object(workload.time, 'monotonic', return_value=17):
            run.record('worker-1', 15)
        s = run.snapshot()['sessions']['worker-1']
        self.assertEqual(s['outage_seconds'], 7)
        self.assertNotIn('secret', str(run.snapshot()))

    def test_validation_and_routes_independent_of_observer(self):
        client = observer.app.test_client()
        data = dict(host='vip', user='writer', password='secret', database='testdb', threads=2)
        with patch.object(observer, 'test_runs', {}), patch.object(workload.TestRun, 'start'):
            self.assertEqual(client.post('/api/tests/primary/start', json=data).status_code, 202)
            self.assertEqual(client.post('/api/tests/readonly/start', json=data).status_code, 202)
            self.assertEqual(len(client.get('/api/tests/status').json), 2)
            self.assertNotIn('secret', client.get('/api/tests/status').text)
            for field, value in [('threads', 0), ('threads', 65), ('sleep', 'NaN'), ('pause', 0), ('database', 'bad`name')]:
                self.assertEqual(client.post('/api/tests/primary/start', json={**data, field: value}).status_code, 400)
            self.assertEqual(client.post('/api/tests/wrong/start', json=data).status_code, 400)
            with patch.object(workload.TestRun, 'stop') as stop:
                self.assertEqual(client.post('/api/tests/primary/stop').status_code, 202)
                stop.assert_called_once()
            page = client.get('/').text
            self.assertIn('role="tablist"', page)
            self.assertIn('>Observer</button>', page)
            self.assertIn('>Test Client</button>', page)


if __name__ == '__main__':
    unittest.main()
