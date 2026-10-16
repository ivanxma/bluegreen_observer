"""Ordered DDL/DML workload with bounded read-node convergence checks."""
from copy import deepcopy
import time
import uuid

from elasticdb_cluster_roles import role_targets, require_role
from elasticdb_test_client import TestRun, open_connection
from mysql_multi_observer import now


class DDLTestRun(TestRun):
    def __init__(self, config):
        super().__init__('ddl', config)
        self.checks = {}
        for session in self.sessions.values():
            session.update(ddl_creates=0, ddl_drops=0, consistency_passes=0,
                           consistency_timeouts=0, table='', phase='Waiting')

    def phase(self, key, phase):
        with self.lock:
            self.sessions[key].update(status=phase, phase=phase)

    def add_count(self, key, name, count=1):
        with self.lock:
            self.sessions[key][name] += count

    def schema(self, con, table):
        with con.cursor() as cur:
            cur.execute('SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, '
                        'CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLUMNS '
                        'WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION',
                        (self.config['database'], table))
            return [tuple(row) for row in cur.fetchall()]

    def contents(self, con, qualified, token, hostname):
        with con.cursor() as cur:
            cur.execute(f'SELECT COUNT(*), COALESCE(MIN(id),0), COALESCE(MAX(id),0), '
                        f'COALESCE(SUM(CASE WHEN BINARY payload=BINARY REPEAT(\'x\', %s) '
                        f'AND BINARY cycle_token=BINARY %s AND BINARY createdby=BINARY %s '
                        f'THEN 1 ELSE 0 END),0) FROM {qualified}',
                        (self.config['width'], token, hostname))
            return tuple(int(value) for value in cur.fetchone())

    def wait_readers(self, key, table, phase, expected=None, token='', hostname='', schema=None):
        began = time.monotonic()
        qualified = f"`{self.config['database']}`.`{table}`"
        self.phase(key, 'Checking readers: ' + phase)
        last_detail = ''
        while not self.stop_event.is_set():
            # Refreshes the same Observer endpoints; promotions/demotions are
            # reflected in the set of required read-only servers on each poll.
            try:
                self.target(key, readonly=True)
                with self.lock:
                    roles = deepcopy(self.roles)
                if any(not server['connected'] for server in roles):
                    raise RuntimeError('An Observer endpoint is unavailable; its current role cannot be verified.')
                targets = role_targets(roles, True)
                all_match = True
                with self.lock:
                    self.checks.pop((key, 'topology', 0, phase), None)
                    endpoints = {(s['host'], s['port']) for s in targets}
                    for old in self.checks.values():
                        if old['worker'] == key and old['phase'] == phase and (old['host'], old['port']) not in endpoints:
                            old['status'] = 'Role changed; no longer a reader'
            except Exception as exc:
                targets, all_match, last_detail = [], False, self.error(exc)
                with self.lock:
                    self.checks[(key, 'topology', 0, phase)] = dict(worker=key, table=table,
                        phase=phase, name='Cluster role discovery', host='', port='', hostname='',
                        status='Waiting for convergence', expected='All Observer endpoints reachable; at least one read-only server',
                        observed=None, elapsed_seconds=round(time.monotonic()-began, 3), observed_at=now(), error=last_detail)
            for target in targets:
                if self.stop_event.is_set():
                    raise InterruptedError('Stopped during reader verification')
                con, matches, observed, error = None, False, None, ''
                try:
                    con = open_connection(dict(self.config, host=target['host'], port=target['port']))
                    require_role(con, True)
                    actual_schema = self.schema(con, table)
                    if expected is None:
                        matches = not actual_schema
                    else:
                        observed = self.contents(con, qualified, token, hostname)
                        matches = actual_schema == schema and observed == expected
                        if actual_schema != schema:
                            error = 'Read-node table structure differs from primary'
                        elif observed != expected:
                            error = 'Read-node row count, ID range or values differ from expected'
                except Exception as exc:
                    error = self.error(exc)
                finally:
                    if con:
                        try:
                            con.close()
                        except Exception:
                            pass
                elapsed = round(time.monotonic() - began, 3)
                result = dict(worker=key, table=table, phase=phase, name=target['name'],
                    host=target['host'], port=target['port'], hostname=target['hostname'],
                    status='Consistent' if matches else 'Waiting for convergence',
                    expected=list(expected) if expected is not None else 'Table absent',
                    observed=list(observed) if observed is not None else None,
                    elapsed_seconds=elapsed, observed_at=now(), error=error)
                with self.lock:
                    check_key = (key, target['host'], target['port'], phase)
                    self.checks[check_key] = result
                all_match = all_match and matches
                if not matches:
                    last_detail = f"{target['name']} {target['host']}:{target['port']}: {error or observed or 'schema differs / table still exists'}"
            if all_match:
                self.add_count(key, 'consistency_passes')
                with self.lock:
                    self.event(key, 'readers_consistent', f'{phase}: {len(targets)} read-only server(s), {time.monotonic()-began:.3f} s')
                return
            if time.monotonic() - began >= self.config['consistency_timeout']:
                self.add_count(key, 'consistency_timeouts')
                with self.lock:
                    for result in self.checks.values():
                        if result['worker'] == key and result['phase'] == phase and result['status'] != 'Consistent':
                            result['status'] = 'Convergence timeout'
                raise TimeoutError(f'{phase}: reader convergence timeout; {last_detail}')
            self.stop_event.wait(.2)
        raise InterruptedError('Stopped during reader verification')

    def worker(self, key):
        if key == 'fresh-login':
            return super().worker(key)
        table = f'elastic_ddl_{self.run_id[:12]}_{key.split("-")[-1]}'
        qualified = f"`{self.config['database']}`.`{table}`"
        with self.lock:
            self.sessions[key]['table'] = qualified
        while not self.stop_event.is_set():
            con, committing, began = None, False, time.monotonic()
            try:
                target = self.target(key, readonly=False)
                con = open_connection(target)
                require_role(con, False)
                with con.cursor() as cur:
                    cur.execute('SELECT @@hostname, CONNECTION_ID(), @@server_uuid')
                    hostname, connection_id, server_uuid = cur.fetchone()
                with self.lock:
                    self.sessions[key].update(hostname=hostname, connection_id=int(connection_id),
                                              host=target['host'], port=target['port'])
                    self.sessions[key]['connections'] += 1
                    self.event(key, 'connection_opened', f'{hostname}, connection {connection_id}')
                token = uuid.uuid4().hex
                total = self.config['batch_size'] * self.config['batches']
                expected = (total, 1, total, total)
                # Only a run-owned table is ever dropped. Initial cleanup also
                # recovers an interrupted or uncertain previous cycle.
                self.phase(key, 'DROP previous cycle')
                with con.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS {qualified}')
                self.add_count(key, 'ddl_drops')
                self.wait_readers(key, table, 'Before CREATE: absent')
                require_role(con, False)
                self.phase(key, 'CREATE table')
                with con.cursor() as cur:
                    cur.execute(f'CREATE TABLE {qualified} (`id` BIGINT NOT NULL PRIMARY KEY, '
                                f'`createdby` VARCHAR(255) NOT NULL, `cycle_token` CHAR(32) NOT NULL, '
                                f'`payload` VARCHAR({self.config["width"]}) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4')
                self.add_count(key, 'ddl_creates')
                schema = self.schema(con, table)
                for batch in range(self.config['batches']):
                    if self.stop_event.is_set():
                        raise InterruptedError('Stopped between batches; run-owned table retained')
                    require_role(con, False)
                    self.phase(key, f'INSERT batch {batch+1}/{self.config["batches"]}')
                    con.start_transaction()
                    rows = [(batch*self.config['batch_size']+i+1, token, self.config['width'])
                            for i in range(self.config['batch_size'])]
                    with con.cursor() as cur:
                        cur.executemany(f'INSERT INTO {qualified} (id, createdby, cycle_token, payload) '
                                        "VALUES (%s, @@hostname, %s, REPEAT('x', %s))", rows)
                    committing = True
                    con.commit()
                    committing = False
                    self.add_count(key, 'committed_rows', self.config['batch_size'])
                    self.add_count(key, 'committed_batches')
                if self.contents(con, qualified, token, hostname) != expected:
                    raise RuntimeError('Primary result does not match the expected rows and values')
                self.wait_readers(key, table, 'After INSERT: schema and data', expected, token, hostname, schema)
                require_role(con, False)
                self.phase(key, 'DROP table')
                with con.cursor() as cur:
                    cur.execute(f'DROP TABLE {qualified}')
                self.add_count(key, 'ddl_drops')
                self.wait_readers(key, table, 'After DROP: absent')
                self.record(key, began)
            except Exception as exc:
                if not self.stop_event.is_set() or committing:
                    self.record(key, began, exc, committing)
                if con:
                    try:
                        con.rollback()
                    except Exception:
                        pass
            finally:
                if con:
                    try:
                        con.close()
                    except Exception:
                        pass
            self.stop_event.wait(self.config['pause'])
        self.phase(key, 'Stopped')

    def snapshot(self):
        result = super().snapshot()
        with self.lock:
            result['checks'] = deepcopy(list(self.checks.values()))
        return result
