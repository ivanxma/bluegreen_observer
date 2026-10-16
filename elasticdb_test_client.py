"""Background workloads for the ElasticDB observer; no observer connections are shared."""
from __future__ import annotations

from collections import deque
from copy import deepcopy
import math
import threading
import time
import uuid

from mysql_multi_observer import IDENTIFIER, now


def config_from_json(data: dict, kind: str) -> dict:
    if kind not in ('primary', 'readonly'):
        raise ValueError('Unknown test case.')
    config = {key: str(data.get(key) or '').strip() for key in ('host', 'user', 'database')}
    config['password'] = str(data.get('password') or '')
    config.update(port=int(data.get('port', 3306)), threads=int(data.get('threads', 1)),
                  sleep=float(data.get('sleep', 10)), pause=float(data.get('pause', .1)))
    if not config['host'] or not config['user'] or not 1 <= config['port'] <= 65535:
        raise ValueError('Enter a host, MySQL user and valid port.')
    if not 1 <= config['threads'] <= 64 or not math.isfinite(config['sleep']) or not .1 <= config['sleep'] <= 3600:
        raise ValueError('Use 1–64 threads and a sleep timer of 0.1–3600 seconds.')
    if not math.isfinite(config['pause']) or not .01 <= config['pause'] <= 60:
        raise ValueError('Use a loop pause of 0.01–60 seconds.')
    if kind == 'primary' and not IDENTIFIER.fullmatch(config['database']):
        raise ValueError('Enter an existing test database (letters, digits and underscores).')
    return config


def open_connection(config: dict, long_query: bool = False):
    import mysql.connector
    return mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'],
        password=config['password'], connection_timeout=2,
        read_timeout=math.ceil(config['sleep']) + 5 if long_query else 5,
        write_timeout=5, autocommit=True)


class TestRun:
    def __init__(self, kind: str, config: dict):
        self.kind, self.config = kind, config
        self.run_id = uuid.uuid4().hex
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.events: deque = deque(maxlen=500)
        self.sessions: dict[str, dict] = {}
        self.active: dict[str, tuple[int, str]] = {}
        self.started_at = now()
        self.stopped_at = ''
        self.stopped_clock: float | None = None
        self.cancel_error = ''
        self.outages: dict[str, float] = {}
        self.table = f"`{config['database']}`.`elastic_observer_test`" if kind == 'primary' else ''
        for key in [f'worker-{i+1}' for i in range(config['threads'])] + ['fresh-login']:
            self.sessions[key] = dict(status='Waiting', successes=0, failures=0, connections=0,
                hostname='', connection_id=None, latency_seconds=None, max_latency_seconds=0,
                last_success_at='', last_failure_at='', last_error='', outage_active=False,
                outage_seconds=None, max_outage_seconds=0, uncertain_commits=0)

    def event(self, key, event, detail=''):
        self.events.append(dict(timestamp_utc=now(), session=key, event=event, detail=detail))

    def start(self):
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        workers = [threading.Thread(target=self.worker, args=(key,), daemon=True) for key in self.sessions]
        try:
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
        finally:
            with self.lock:
                self.stopped_at = now()
                self.stopped_clock = time.monotonic()
                self.event('test', 'stopped')

    def stop(self):
        self.stop_event.set()
        # KILL QUERY uses a separate connection and the same account. Workers retain
        # ownership of their connections and close them after the query is interrupted.
        if self.kind == 'readonly':
            threading.Thread(target=self.cancel_queries, daemon=True).start()

    def cancel_queries(self):
        control = None
        try:
            control = open_connection(self.config)
            with control.cursor() as cur:
                cur.execute('SELECT @@server_uuid')
                server_uuid = str(cur.fetchone()[0])
                while True:
                    with self.lock:
                        targets = [value for key, value in self.active.items() if key != 'fresh-login']
                        if not targets:
                            break
                        for connection_id, target_uuid in targets:
                            if target_uuid != server_uuid:
                                self.cancel_error = 'Stop connection reached a different backend; waiting for SLEEP to finish.'
                                return
                            cur.execute(f'KILL QUERY {int(connection_id)}')
                    # Retry across the small gap between session registration and SLEEP.
                    threading.Event().wait(.1)
        except Exception as exc:
            with self.lock:
                self.cancel_error = self.error(exc)
        finally:
            if control:
                control.close()

    def error(self, exc):
        message = f'{type(exc).__name__}: {exc}'
        password = self.config['password']
        return message.replace(password, '[redacted]') if password else message

    def record(self, key, began, error=None, uncertain=False):
        with self.lock:
            s = self.sessions[key]
            elapsed = round(time.monotonic() - began, 3)
            s['latency_seconds'] = elapsed
            s['max_latency_seconds'] = max(s['max_latency_seconds'], elapsed)
            if error:
                s['failures'] += 1
                s['uncertain_commits'] += int(uncertain)
                s['status'] = 'Reconnecting'
                s['last_error'], s['last_failure_at'] = self.error(error), now()
                if not s['outage_active']:
                    self.outages[key] = began
                    s['outage_active'] = True
                    self.event(key, 'failure', s['last_error'])
            else:
                s['successes'] += 1
                s['last_success_at'] = now()
                s['status'] = 'Connected'
                if s['outage_active']:
                    s['outage_seconds'] = round(time.monotonic() - self.outages.pop(key), 3)
                    s['max_outage_seconds'] = max(s['max_outage_seconds'], s['outage_seconds'])
                    s['outage_active'] = False
                    self.event(key, 'recovered', f"Interruption {s['outage_seconds']} s")

    def worker(self, key):
        con = None
        prepared = False
        fresh = key == 'fresh-login'
        while not self.stop_event.is_set():
            began, committing = time.monotonic(), False
            try:
                if con is None:
                    con = open_connection(self.config, long_query=not fresh and self.kind == 'readonly')
                    with con.cursor() as cur:
                        cur.execute('SELECT @@hostname, CONNECTION_ID(), @@server_uuid')
                        hostname, connection_id, server_uuid = cur.fetchone()
                    with self.lock:
                        s = self.sessions[key]
                        s.update(hostname=hostname, connection_id=int(connection_id))
                        s['connections'] += 1
                        self.active[key] = (int(connection_id), str(server_uuid))
                        self.event(key, 'connection_opened', f'{hostname}, connection {connection_id}')
                if self.stop_event.is_set():
                    break
                with self.lock:
                    self.sessions[key]['status'] = 'Connecting / SELECT 1' if fresh else 'Running transaction' if self.kind == 'primary' else 'Running SLEEP'
                with con.cursor() as cur:
                    if fresh:
                        cur.execute('SELECT 1')
                        cur.fetchone()
                    elif self.kind == 'readonly':
                        cur.execute('SELECT SLEEP(%s)', (self.config['sleep'],))
                        if cur.fetchone()[0] != 0:
                            raise RuntimeError('SLEEP was interrupted')
                    else:
                        if not prepared:
                            cur.execute(f'CREATE TABLE IF NOT EXISTS {self.table} (client_id VARCHAR(80) PRIMARY KEY, counter BIGINT NOT NULL, updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)) ENGINE=InnoDB')
                            prepared = True
                        con.start_transaction()
                        client_id = self.run_id + '-' + key
                        cur.execute(f'INSERT INTO {self.table} (client_id,counter) VALUES (%s,0) ON DUPLICATE KEY UPDATE counter=counter', (client_id,))
                        cur.execute(f'UPDATE {self.table} SET counter=counter+1 WHERE client_id=%s', (client_id,))
                        cur.execute(f'SELECT counter FROM {self.table} WHERE client_id=%s', (client_id,))
                        cur.fetchone()
                        committing = True
                        con.commit()
                        committing = False
                self.record(key, began)
            except Exception as exc:
                if not self.stop_event.is_set() or committing:
                    self.record(key, began, exc, committing)
                if con:
                    try:
                        con.rollback()
                    except Exception:
                        pass
                self.close(key, con)
                con = None
                prepared = False
            finally:
                if fresh:
                    self.close(key, con)
                    con = None
            self.stop_event.wait(1 if fresh or con is None else self.config['pause'])
        self.close(key, con)
        with self.lock:
            self.sessions[key]['status'] = 'Stopped'

    def close(self, key, con):
        # Serialize removal/close with KILL QUERY so an old ID cannot be reused.
        with self.lock:
            self.active.pop(key, None)
            if con:
                try:
                    con.close()
                except Exception:
                    pass

    def snapshot(self):
        with self.lock:
            sessions = deepcopy(self.sessions)
            for key, began in self.outages.items():
                sessions[key]['outage_seconds'] = round((self.stopped_clock or time.monotonic()) - began, 3)
                if self.stopped_clock is not None:
                    sessions[key]['outage_active'] = False
                sessions[key]['max_outage_seconds'] = max(sessions[key]['max_outage_seconds'], sessions[key]['outage_seconds'])
            return dict(kind=self.kind, run_id=self.run_id, running=bool(self.thread and self.thread.is_alive()),
                stopping=self.stop_event.is_set() and not self.stopped_at, started_at=self.started_at,
                stopped_at=self.stopped_at, host=self.config['host'], port=self.config['port'],
                sessions=sessions, events=list(self.events), cancel_error=self.cancel_error)
