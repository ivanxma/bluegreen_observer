from copy import deepcopy
import re
import unittest
from unittest.mock import patch

import elasticdb_test_client as workload
import elasticdb_ddl_test as ddl
from elasticdb_cluster_roles import discover_roles, role_targets


class World:
    def __init__(self):
        self.roles = {'primary': False, 'node1': True, 'node2': True}
        self.tables = {host: {} for host in self.roles}
        self.offline = set()
        self.operations = []
        self.delay = 0
        self.corrupt = False
        self.commit_error = False
        self.closed = []

    def connect(self, config, **kwargs):
        host = config['host']
        if host in self.offline:
            raise ConnectionError('offline')
        return Connection(self, host)

    def config(self, kind='ddl'):
        config = workload.config_from_json(dict(host='primary', user='tester', password='secret',
            database='testdb', threads=1, batch_size=3, batches=2, width=20,
            consistency_timeout=.1, pause=.01), kind)
        config['servers'] = [dict(name=host, host=host, port=3306) for host in self.roles]
        config['roles'] = discover_roles(config, self.connect)
        return config


class Connection:
    def __init__(self, world, host):
        self.world, self.host = world, host

    def cursor(self):
        return Cursor(self)

    def start_transaction(self):
        self.world.operations.append((self.host, 'BEGIN'))

    def commit(self):
        self.world.operations.append((self.host, 'COMMIT'))
        if self.world.commit_error:
            raise ConnectionError('commit response lost')

    def rollback(self):
        self.world.operations.append((self.host, 'ROLLBACK'))

    def close(self):
        self.world.closed.append(self.host)


class Cursor:
    def __init__(self, con):
        self.con = con
        self.world, self.host = con.world, con.host
        self.sql, self.params = '', None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params
        self.world.operations.append((self.host, sql))
        names = re.findall(r'`testdb`\.`([^`]+)`', sql)
        if names:
            self.table = names[0]
        if sql.startswith('DROP TABLE'):
            self.world.tables[self.host].pop(self.table, None)
        if sql.startswith('CREATE TABLE'):
            self.world.tables[self.host][self.table] = dict(rows=[], width=20)

    def executemany(self, sql, rows):
        self.execute(sql)
        self.world.tables[self.host][self.table]['rows'].extend(
            (id, self.host, token, width) for id, token, width in rows)

    def fetchone(self):
        ro = int(self.world.roles[self.host])
        if self.sql.startswith('SELECT @@hostname, @@server_uuid'):
            return (self.host, 'uuid-'+self.host, ro, ro)
        if 'CONNECTION_ID' in self.sql:
            return (self.host, 123, 'uuid-'+self.host)
        if self.sql.startswith('SELECT @@global.read_only'):
            return (ro, ro)
        if self.sql.startswith('SELECT COUNT'):
            table = self.world.tables[self.host].get(self.table)
            if table is None:
                raise RuntimeError('table not yet present')
            rows = table['rows']
            width, token, host = self.params
            count = len(rows)
            matched = sum(r[1] == host and r[2] == token and r[3] == width for r in rows)
            if self.world.corrupt and self.host != 'primary':
                matched = 0
            return (count, min((r[0] for r in rows), default=0), max((r[0] for r in rows), default=0), matched)
        return (1,)

    def fetchall(self):
        if 'information_schema.COLUMNS' in self.sql:
            _, table = self.params
            if self.host != 'primary':
                if self.world.delay > 0 and self.world.tables['primary'].get(table, {}).get('rows'):
                    self.world.delay -= 1
                else:
                    self.world.tables[self.host] = deepcopy(self.world.tables['primary'])
            if table not in self.world.tables[self.host]:
                return []
            return [('id','bigint','NO','PRI','','',''), ('payload','varchar(20)','NO','','','utf8mb4','utf8mb4_0900_ai_ci')]
        return []


class ClusterWorkloadTests(unittest.TestCase):
    def test_roles_detect_and_follow_promotion(self):
        world = World()
        config = world.config('readonly')
        run = workload.TestRun('readonly', config)
        with patch.object(workload, 'open_connection', side_effect=world.connect):
            self.assertEqual(run.target('worker-1')['host'], 'node1')
            self.assertEqual(run.target('worker-2')['host'], 'node2')
            world.roles = {'primary': True, 'node1': False, 'node2': True}
            run.roles_sampled = 0
            self.assertEqual(run.target('worker-1', readonly=False)['host'], 'node1')
            self.assertEqual(run.target('worker-1', readonly=True)['host'], 'primary')

    def test_aliases_deduplicated_and_ambiguous_writer_rejected(self):
        world = World()
        roles = world.config()['roles']
        self.assertEqual(len(role_targets(roles+[dict(roles[0], host='vip')], False)), 1)
        roles[1]['read_only'] = False
        with self.assertRaisesRegex(RuntimeError, 'Multiple'):
            role_targets(roles, False)
        roles[0]['connected'] = False
        roles[1]['connected'] = False
        with self.assertRaisesRegex(RuntimeError, 'No reachable writable'):
            role_targets(roles, False)

    def run_one_cycle(self, world):
        run = ddl.DDLTestRun(world.config())
        original = run.record
        def stop_after_result(*args, **kwargs):
            original(*args, **kwargs)
            run.stop_event.set()
        with patch.object(workload, 'open_connection', side_effect=world.connect), patch.object(ddl, 'open_connection', side_effect=world.connect), patch.object(run, 'record', side_effect=stop_after_result):
            run.worker('worker-1')
        return run

    def test_ddl_order_and_all_readers_schema_data_drop(self):
        world = World()
        run = self.run_one_cycle(world)
        result = run.snapshot()
        s = result['sessions']['worker-1']
        self.assertEqual((s['successes'], s['failures']), (1, 0))
        self.assertEqual((s['ddl_creates'], s['ddl_drops'], s['committed_batches'], s['committed_rows']), (1, 2, 2, 6))
        self.assertEqual(s['consistency_passes'], 3)
        self.assertEqual(len(result['checks']), 6)
        self.assertTrue(all(c['status'] == 'Consistent' for c in result['checks']))
        self.assertEqual({c['host'] for c in result['checks']}, {'node1','node2'})
        self.assertTrue(all(not tables for tables in world.tables.values()))
        actions = [sql.split()[0] for host,sql in world.operations if host=='primary' and sql.startswith(('CREATE','INSERT','DROP','COMMIT'))]
        self.assertEqual(actions, ['DROP','CREATE','INSERT','COMMIT','INSERT','COMMIT','DROP'])
        self.assertNotIn('secret', str(result))

    def test_delayed_reader_converges(self):
        world = World()
        world.delay = 1
        run = self.run_one_cycle(world)
        self.assertEqual(run.snapshot()['sessions']['worker-1']['successes'], 1)
        self.assertEqual(run.snapshot()['sessions']['worker-1']['consistency_timeouts'], 0)

    def test_wrong_payload_is_timeout_not_consistent(self):
        world = World()
        world.corrupt = True
        run = self.run_one_cycle(world)
        s = run.snapshot()['sessions']['worker-1']
        self.assertEqual((s['successes'], s['failures'], s['consistency_timeouts']), (0,1,1))
        self.assertTrue(any(c['status']=='Convergence timeout' for c in run.snapshot()['checks']))
        self.assertEqual(s['ddl_drops'], 1)  # Data retained until a later cleanup attempt.

    def test_unreachable_node_cannot_pass_consistency(self):
        world = World()
        world.offline.add('node2')
        run = self.run_one_cycle(world)
        s = run.snapshot()['sessions']['worker-1']
        self.assertEqual(s['successes'], 0)
        self.assertEqual(s['consistency_timeouts'], 1)
        self.assertIn('unavailable', s['last_error'])

    def test_ddl_commit_error_is_uncertain_not_counted(self):
        world = World()
        world.commit_error = True
        run = self.run_one_cycle(world)
        s = run.snapshot()['sessions']['worker-1']
        self.assertEqual((s['uncertain_commits'],s['committed_rows'],s['committed_batches']), (1,0,0))
        self.assertTrue(any(sql=='ROLLBACK' for _,sql in world.operations))


if __name__ == '__main__':
    unittest.main()
