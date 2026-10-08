#!/usr/bin/env python3
"""ElasticDBCluster dashboard with fresh and persistent MySQL connections.

Run: python elasticdb_cluster_observer.py (http://127.0.0.1:5052).
Shared server sampling, events, table reads and LSN tracking live in
mysql_multi_observer.py. This observer never changes database state.
"""
from __future__ import annotations

import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, replace
from typing import Any

from flask import Flask, jsonify, request

from mysql_multi_observer import (
    IDENTIFIER, ROW_LIMIT, MultiMonitor, PAGE as MULTI_PAGE,
    ServerSample, connect, now, tables_for,
)

app = Flask(__name__)
app_lock = threading.Lock()
monitor: ElasticDBClusterMonitor | None = None


def airglow_attributes(cur: Any) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    attributes, errors = {}, {}
    for kind in ("status", "variables"):
        try:
            cur.execute(f"SHOW GLOBAL {kind.upper()} LIKE '%airglow%'")
            attributes[kind] = {str(name): str(value) for name, value in cur.fetchall()}
        except Exception as exc:
            attributes[kind] = {}
            errors[kind] = f"{type(exc).__name__}: {exc}"
    return attributes, errors


@dataclass
class ElasticServerSample(ServerSample):
    airglow: dict[str, dict[str, str]] = field(default_factory=dict)
    airglow_errors: dict[str, str] = field(default_factory=dict)


@dataclass
class CachedSample:
    id: str
    name: str
    host: str
    port: int
    connected: bool = False
    status: str = "Waiting for first sample"
    uptime_seconds: int | None = None
    observed_at: str = ""
    last_outage_at: str = ""
    last_restored_at: str = ""
    outage_duration_seconds: float | None = None
    outage_active: bool = False
    error: str = ""
    last_outage_error: str = ""


class ElasticDBClusterMonitor(MultiMonitor):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.cached_servers = [CachedSample(s.id, s.name, s.host, s.port) for s in self.servers]
        # Each connection belongs to one server worker, used once per sample cycle.
        self.connections: dict[str, Any] = {}
        self.outage_started: dict[str, float] = {}

    def make_sample(self, old: ServerSample) -> ElasticServerSample:
        return ElasticServerSample(old.id, old.name, old.host, old.port, observed_at=now())

    def sample_extra(self, cur: Any, fresh: ElasticServerSample) -> None:
        fresh.airglow, fresh.airglow_errors = airglow_attributes(cur)

    def close_connection(self, server_id: str) -> None:
        connection = self.connections.pop(server_id, None)
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass

    def query_cached(self, old: CachedSample) -> CachedSample:
        fresh = replace(old, observed_at=now())
        opening = old.id not in self.connections
        try:
            if opening:
                self.connections[old.id] = connect(
                    old.host, old.port, self.config["user"], self.config["password"])
            con = self.connections[old.id]
            # No implicit reconnect: a failed health check must be recorded.
            with con.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.execute("SHOW GLOBAL STATUS LIKE 'Uptime'")
                row = cur.fetchone()
                fresh.uptime_seconds = int(row[1]) if row else None
            fresh.observed_at = now()
            fresh.connected = True
            fresh.status = "Connected"
            fresh.error = ""
            if fresh.outage_active:
                fresh.outage_duration_seconds = round(
                    time.monotonic() - self.outage_started.pop(old.id), 3)
                fresh.last_restored_at = fresh.observed_at
                fresh.outage_active = False
        except Exception as exc:
            self.close_connection(old.id)
            fresh.observed_at = now()
            fresh.connected = False
            fresh.status = "Reconnecting"
            fresh.uptime_seconds = None
            fresh.error = f"{type(exc).__name__}: {exc}"
            if not fresh.outage_active:
                fresh.last_outage_at = fresh.observed_at
                fresh.last_restored_at = ""
                fresh.outage_active = True
                self.outage_started[old.id] = time.monotonic()
            fresh.last_outage_error = fresh.error
            fresh.outage_duration_seconds = round(
                time.monotonic() - self.outage_started[old.id], 3)
        return fresh

    def run(self) -> None:
        try:
            with ThreadPoolExecutor(max_workers=min(len(self.servers) * 2, 32)) as pool:
                while not self.stop_event.is_set():
                    began = time.monotonic()
                    with self.lock:
                        originals = list(self.servers)
                        cached_originals = list(self.cached_servers)
                    futures = {pool.submit(self.query, s): ("fresh", s.id) for s in originals}
                    futures.update({pool.submit(self.query_cached, s): ("cached", s.id)
                                    for s in cached_originals})
                    updates, cached_updates, sampled = {}, {}, {}
                    for future in as_completed(futures):
                        mode, server_id = futures[future]
                        if mode == "fresh":
                            updates[server_id] = future.result()
                            sampled[server_id] = (time.monotonic(), now())
                        else:
                            cached_updates[server_id] = future.result()
                    with self.lock:
                        for index, old in enumerate(self.servers):
                            fresh = updates[old.id]
                            self.evaluate(old, fresh)
                            self.servers[index] = fresh
                        for index, old in enumerate(self.cached_servers):
                            fresh = cached_updates[old.id]
                            if not old.outage_active and fresh.outage_active:
                                self.log(fresh.name, "cached_connection_lost", fresh.error)
                            elif old.outage_active and not fresh.outage_active:
                                self.log(fresh.name, "cached_connection_restored",
                                         f"Outage: {fresh.outage_duration_seconds:.3f} s")
                            elif not old.observed_at and fresh.connected:
                                self.log(fresh.name, "cached_connection_opened")
                            self.cached_servers[index] = fresh
                        self.update_propagation(sampled)
                    self.stop_event.wait(max(0, self.config["interval"] - (time.monotonic() - began)))
        finally:
            for server_id in list(self.connections):
                self.close_connection(server_id)
            with self.lock:
                for sample in self.cached_servers:
                    sample.connected = False
                    sample.status = "Stopped"

    def snapshot(self) -> dict[str, Any]:
        result = super().snapshot()
        with self.lock:
            result["cached_servers"] = [asdict(s) for s in self.cached_servers]
        return result


# Keep the shared setup, events, selected tables and LSN view.
PAGE = MULTI_PAGE.replace("MySQL multi-server observer", "ElasticDBCluster Observer")
PAGE = PAGE.replace(
    "Read-only observation of server status and performance_schema tables.",
    "Compare fresh connections and cached connections across your cluster.")
PAGE = PAGE.replace('<div class="layout"><aside class="left" id="servers"><section class="panel muted">Server status appears here after Start observe.</section></aside><div class="right">', '''
<section class="panel statuspanel"><h2>Server status · New connection per sample</h2>
<p class="muted">Each status sample opens a new connection to each server, reads its attributes and selected tables, then closes it.</p>
<div id="servers">Server status appears here after Start observe.</div></section>
<section class="panel statuspanel"><h2>Cached connections</h2>
<p class="muted">One connection per server is reused. Failed connections are closed; reconnection is attempted on the next sample. Uptime is the server uptime.</p>
<div id="cachedServers">Cached connection status appears here after Start observe.</div>
<p class="muted">Outage times are UTC. Duration runs from the first failed check to the first successful reconnect; active outages show the duration at the last check.</p></section>
<div class="layout"><div class="right">''')
PAGE = PAGE.replace('</style>', '''
.layout{display:block}.right{grid-template-rows:minmax(230px,35vh) minmax(320px,1fr)}
.statuspanel{margin-bottom:1rem}.statuspanel p{margin-bottom:.7rem}
.statusgrid{border-collapse:collapse;width:100%;font-size:.85rem}
.statusgrid th,.statusgrid td{padding:.55rem;border:1px solid #dce4ed;text-align:left;vertical-align:top}
.statusgrid thead th{background:#eef3f8;min-width:230px}
.statusgrid th:first-child{position:sticky;left:0;background:#eef3f8;min-width:170px;z-index:1}
.statusgrid td{min-width:230px;max-width:420px;overflow-wrap:anywhere;white-space:normal}
.statusgrid .serverhead{background:hsl(var(--server-hue) 70% 92%);border-top:4px solid hsl(var(--server-hue) 65% 43%)}
</style>''')
card_start = PAGE.index('function serverCard(')
card_end = PAGE.index('function tableBlock(', card_start)
PAGE = PAGE[:card_start] + r'''
function connectionStatus(s){return `<span class="${s.connected?'ok':s.observed_at?'error':'muted'}">${esc(s.status||(s.observed_at?(s.connected?'Connected':'No connection'):'Waiting for first sample'))}</span>`}
function attributeTable(servers,rows){return `<table class="statusgrid"><thead><tr><th scope="col">Attribute</th>${servers.map(s=>`<th scope="col" class="serverhead" ${serverStyle(s)}>${esc(s.name)}${s.id==='s1'?' (Primary)':''}<br><small>${esc(s.host)}:${esc(s.port)}</small></th>`).join('')}</tr></thead><tbody>${rows.map(([label,value])=>`<tr><th scope="row">${label}</th>${servers.map(s=>`<td>${value(s)}</td>`).join('')}</tr>`).join('')}</tbody></table>`}
function airglowRows(servers){return ['status','variables'].flatMap(kind=>{
 const names=[...new Set(servers.flatMap(s=>Object.keys(s.airglow?.[kind]||{})))].sort();
 const rows=names.map(name=>[`${kind==='status'?'Status':'Variable'} · ${esc(name)}`,s=>esc(s.airglow_errors?.[kind]||s.airglow?.[kind]?.[name]||'—')]);
 if(servers.some(s=>s.airglow_errors?.[kind]))rows.push([`Airglow ${kind} query error`,s=>esc(s.airglow_errors?.[kind]||'—')]);
 if(!names.length)rows.push([`Airglow ${kind}`,s=>esc(s.airglow_errors?.[kind]||(s.connected?'No matching attributes':'Unavailable'))]);
 return rows;
})}
function serverTable(servers){return attributeTable(servers,[
 ['Status',connectionStatus],['Hostname',s=>esc(s.hostname)],['Uptime',s=>uptime(s.uptime_seconds)],
 ['Read only',s=>s.read_only==null?'—':s.read_only?'YES':'NO (RW)'],
 ['GTID executed',s=>esc(s.gtid_error||s.gtid_executed||'(empty)')],
 ['Server UUID',s=>esc(s.server_uuid||'—')],['InnoDB LSN',s=>esc(s.log_status_error||s.innodb_lsn)],
 ['InnoDB LSN checkpoint',s=>esc(s.innodb_lsn_checkpoint)],['Replication',s=>esc(s.replication)],
 ['Error',s=>esc(s.error||'—')],['Last sample (UTC)',s=>esc(s.observed_at||'—')],...airglowRows(servers)])}
function cachedTable(servers){return attributeTable(servers,[
 ['Uptime',s=>uptime(s.uptime_seconds)],['Status',connectionStatus],
 ['Last outage time (UTC)',s=>esc(s.last_outage_at||'—')],
 ['Outage duration',s=>s.outage_duration_seconds==null?'—':`${Number(s.outage_duration_seconds).toFixed(3)} s${s.outage_active?' (ongoing)':''}`],
 ['Last restored (UTC)',s=>esc(s.last_restored_at||'—')],
 ['Outage event',s=>esc(s.error||s.last_outage_error||'—')]])}
''' + PAGE[card_end:]
PAGE = PAGE.replace("$('#servers').innerHTML=data.servers.map(serverCard).join('');",
                    "$('#servers').innerHTML=serverTable(data.servers);$('#cachedServers').innerHTML=cachedTable(data.cached_servers||[]);")
PAGE = PAGE.replace('Load tables to select what to monitor.', 'Optionally load tables to select what to monitor.')


@app.get("/")
def index():
    return PAGE


@app.post("/api/tables")
def discover_tables():
    data = request.get_json(silent=True) or {}
    try:
        host = str(data["host"]).strip()
        port = int(data.get("port") or 3306)
        user = str(data["user"]).strip()
        password = str(data.get("password") or "")
        if not host or not user or not 1 <= port <= 65535:
            raise ValueError
    except (KeyError, TypeError, ValueError):
        return jsonify(error="Enter a valid host, port, and MySQL user."), 400
    try:
        return jsonify(host=host, tables=tables_for(host, port, user, password))
    except Exception as exc:
        return jsonify(error=f"Could not list tables on {host}: {type(exc).__name__}: {exc}"), 502


@app.post("/api/start")
def start():
    global monitor
    data = request.get_json(silent=True) or {}
    try:
        servers = data["servers"]
        tables = data["tables"]
        user = str(data["user"]).strip()
        password = str(data.get("password") or "")
        interval = float(data.get("interval") or 1)
        if not isinstance(servers, list) or not 1 <= len(servers) <= 16 or not isinstance(tables, list) or not 0 <= len(tables) <= 8:
            raise ValueError
        clean_servers = []
        for item in servers:
            name, host, port = str(item["name"]).strip(), str(item["host"]).strip(), int(item["port"])
            if not name or not host or not 1 <= port <= 65535:
                raise ValueError
            clean_servers.append({"name": name, "host": host, "port": port})
        if len({(s["host"], s["port"]) for s in clean_servers}) != len(clean_servers):
            return jsonify(error="Each DB server must have a unique host and port."), 400
        if not user or not .1 <= interval <= 60 or any(not isinstance(t, str) or not IDENTIFIER.fullmatch(t) for t in tables):
            raise ValueError
        tables = list(dict.fromkeys(tables))
    except (KeyError, TypeError, ValueError):
        return jsonify(error="Enter 1–16 unique DB servers, 0–8 tables, a MySQL user, and a 0.1–60 second interval."), 400
    with app_lock:
        if monitor and monitor.thread and monitor.thread.is_alive():
            return jsonify(error="Wait for the current observer to finish stopping before starting another."), 409
        monitor = ElasticDBClusterMonitor({"servers": clean_servers, "tables": tables, "user": user,
                                "password": password, "interval": interval})
        monitor.start()
    return jsonify(ok=True)


@app.post("/api/stop")
def stop():
    with app_lock:
        if monitor:
            monitor.stop_event.set()
    return jsonify(ok=True)


@app.get("/api/status")
def status():
    with app_lock:
        current = monitor
    return jsonify(current.snapshot() if current else {"running": False, "servers": [], "events": [],
                                                     "row_limit": ROW_LIMIT, "propagation_rows": [],
                                                     "selected_tables": [], "cached_servers": []})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5052)
    args = parser.parse_args()
    app.run(host="127.0.0.1", port=args.port, debug=False)
