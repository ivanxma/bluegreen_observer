#!/usr/bin/env python3
"""Observe multiple MySQL servers and selected performance_schema tables.

Run: python mysql_multi_observer.py  (then open http://127.0.0.1:5051)
This dashboard only reads from MySQL; it never performs a failover.
"""
from __future__ import annotations

import argparse
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)
app_lock = threading.Lock()
monitor: MultiMonitor | None = None
IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
ROW_LIMIT = 50
EVENT_LIMIT = 1000
PROPAGATION_LIMIT = 1000


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def connect(host: str, port: int, user: str, password: str, timeout: int = 2):
    import mysql.connector

    return mysql.connector.connect(
        host=host, port=port, user=user, password=password,
        connection_timeout=timeout, read_timeout=timeout, write_timeout=timeout,
        autocommit=True,
    )


def tables_for(host: str, port: int, user: str, password: str) -> list[str]:
    con = connect(host, port, user, password)
    try:
        with con.cursor() as cur:
            cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA = 'performance_schema' ORDER BY TABLE_NAME")
            return [row[0] for row in cur.fetchall()]
    finally:
        con.close()


def cell(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, bytes):
        return value.hex() if len(value) > 100 else value.decode("utf-8", "replace")
    return str(value)[:500]


def innodb_log_positions(storage_engines: Any) -> tuple[int | None, int | None]:
    if isinstance(storage_engines, bytes):
        storage_engines = storage_engines.decode("utf-8")
    if isinstance(storage_engines, str):
        storage_engines = json.loads(storage_engines)
    engine = storage_engines.get("InnoDB") if isinstance(storage_engines, dict) else None
    if not isinstance(engine, dict):
        return None, None
    lsn = engine.get("LSN")
    checkpoint = engine.get("LSN_checkpoint")
    return (int(lsn) if lsn is not None else None,
            int(checkpoint) if checkpoint is not None else None)


@dataclass
class TableSample:
    columns: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    error: str = ""
    observed_at: str = ""


def table_values_changed(before: TableSample, after: TableSample) -> bool:
    # A table read has no ORDER BY, so row order alone is not a content change.
    return before.columns != after.columns or sorted(json.dumps(row) for row in before.rows) != sorted(
        json.dumps(row) for row in after.rows
    )


@dataclass
class ServerSample:
    id: str
    name: str
    host: str
    port: int
    connected: bool = False
    hostname: str = "—"
    uptime_seconds: int | None = None
    gtid_executed: str = ""
    gtid_error: str = ""
    server_uuid: str = ""
    innodb_lsn: int | None = None
    innodb_lsn_checkpoint: int | None = None
    log_status_error: str = ""
    read_only: bool | None = None
    replication: str = "Unknown / not available"
    replication_channels: dict[str, str] = field(default_factory=dict)
    replication_available: bool = False
    tables: dict[str, TableSample] = field(default_factory=dict)
    error: str = ""
    observed_at: str = ""


@dataclass
class Event:
    timestamp_utc: str
    elapsed_seconds: float
    server: str
    event: str
    detail: str


class MultiMonitor:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.servers = [ServerSample(f"s{i+1}", s["name"], s["host"], s["port"])
                        for i, s in enumerate(config["servers"])]
        self.events: list[Event] = []
        self.last_connected: dict[str, ServerSample] = {}
        self.primary_lsn: int | None = None
        self.propagation_rows: list[dict[str, Any]] = []
        self.started = time.monotonic()
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def log(self, server: str, event: str, detail: str = "") -> None:
        self.events.append(Event(now(), round(time.monotonic() - self.started, 3), server, event, detail))
        if len(self.events) > EVENT_LIMIT:
            del self.events[:len(self.events) - EVENT_LIMIT]

    def query(self, old: ServerSample) -> ServerSample:
        fresh = ServerSample(old.id, old.name, old.host, old.port, observed_at=now())
        try:
            con = connect(old.host, old.port, self.config["user"], self.config["password"])
            try:
                with con.cursor() as cur:
                    cur.execute("SELECT @@hostname, @@global.read_only, COALESCE(@@global.super_read_only, 0)")
                    hostname, ro, super_ro = cur.fetchone()
                    fresh.connected = True
                    fresh.hostname = str(hostname)
                    fresh.read_only = bool(ro or super_ro)
                    try:
                        cur.execute("SHOW GLOBAL STATUS LIKE 'Uptime'")
                        row = cur.fetchone()
                        fresh.uptime_seconds = int(row[1]) if row else None
                    except Exception:
                        pass
                    try:
                        cur.execute("SELECT @@GLOBAL.gtid_executed")
                        row = cur.fetchone()
                        fresh.gtid_executed = str(row[0] or "") if row else ""
                    except Exception as exc:
                        fresh.gtid_error = f"{type(exc).__name__}: {exc}"
                    try:
                        cur.execute("SELECT SERVER_UUID, STORAGE_ENGINES FROM performance_schema.log_status")
                        row = cur.fetchone()
                        if row:
                            fresh.server_uuid = str(row[0] or "")
                            fresh.innodb_lsn, fresh.innodb_lsn_checkpoint = innodb_log_positions(row[1])
                        else:
                            fresh.log_status_error = "No log status returned"
                    except Exception as exc:
                        fresh.log_status_error = f"{type(exc).__name__}: {exc}"
                    try:
                        cur.execute("SELECT CHANNEL_NAME, SERVICE_STATE FROM performance_schema.replication_connection_status")
                        fresh.replication_channels = {str(name or "default"): str(state) for name, state in cur.fetchall()}
                        fresh.replication_available = True
                        fresh.replication = ", ".join(f"{k}: {v}" for k, v in fresh.replication_channels.items()) or "No replication channels"
                    except Exception:
                        fresh.replication = "Not available (version or privileges)"
                    for table in self.config["tables"]:
                        sample = TableSample(observed_at=now())
                        try:
                            # Identifiers are restricted at the API boundary; parameters cannot bind identifiers.
                            cur.execute(f"SELECT * FROM `performance_schema`.`{table}` LIMIT {ROW_LIMIT}")
                            sample.columns = list(cur.column_names)
                            sample.rows = [[cell(value) for value in row] for row in cur.fetchall()]
                        except Exception as exc:
                            sample.error = f"{type(exc).__name__}: {exc}"
                        fresh.tables[table] = sample
            finally:
                con.close()
        except Exception as exc:
            fresh.error = f"{type(exc).__name__}: {exc}"
            fresh.tables = {table: TableSample(error="Server unavailable", observed_at=fresh.observed_at)
                            for table in self.config["tables"]}
        return fresh

    def evaluate(self, old: ServerSample, fresh: ServerSample) -> None:
        name = fresh.name
        baseline = old if old.connected else self.last_connected.get(old.id)
        if not old.observed_at:
            self.log(name, "initial_status", f"connected={fresh.connected}; hostname={fresh.hostname}; read_only={fresh.read_only}")
        else:
            if old.connected != fresh.connected:
                self.log(name, "connection_restored" if fresh.connected else "connection_lost", fresh.hostname if fresh.connected else fresh.error)
            if not old.connected and not fresh.connected and old.error != fresh.error:
                self.log(name, "connection_error_changed", f"{old.error} → {fresh.error}")
            if baseline and fresh.connected:
                if baseline.hostname != fresh.hostname:
                    self.log(name, "hostname_changed", f"{baseline.hostname} → {fresh.hostname}")
                if baseline.read_only != fresh.read_only:
                    self.log(name, "read_only_changed", f"{baseline.read_only} → {fresh.read_only}")
                if baseline.uptime_seconds is not None and fresh.uptime_seconds is not None and fresh.uptime_seconds < baseline.uptime_seconds:
                    self.log(name, "uptime_reset", f"{baseline.uptime_seconds} → {fresh.uptime_seconds} seconds")
                if (baseline.uptime_seconds is None) != (fresh.uptime_seconds is None):
                    self.log(name, "uptime_availability_changed", f"{baseline.uptime_seconds} → {fresh.uptime_seconds}")
                if baseline.gtid_error != fresh.gtid_error:
                    self.log(name, "gtid_access_changed", fresh.gtid_error or "GTID available")
                if not baseline.gtid_error and not fresh.gtid_error and baseline.gtid_executed != fresh.gtid_executed:
                    self.log(name, "gtid_executed_changed", f"{baseline.gtid_executed or '(empty)'} → {fresh.gtid_executed or '(empty)'}")
                if baseline.replication_available != fresh.replication_available:
                    self.log(name, "replication_access_changed", fresh.replication)
                if baseline.replication_available and fresh.replication_available:
                    for channel in sorted(baseline.replication_channels.keys() | fresh.replication_channels.keys()):
                        before = baseline.replication_channels.get(channel)
                        after = fresh.replication_channels.get(channel)
                        if before != after:
                            self.log(name, "replication_channel_changed", f"{channel}: {before or 'absent'} → {after or 'absent'}")
        for table, sample in fresh.tables.items():
            prior = old.tables.get(table)
            if not prior:
                if sample.error:
                    self.log(name, "table_error", f"{table}: {sample.error}")
            elif prior.error != sample.error:
                self.log(name, "table_error" if sample.error else "table_restored", f"{table}: {sample.error or 'readable'}")
            elif not sample.error and table_values_changed(prior, sample):
                self.log(name, "table_changed", f"{table}: sampled content changed ({len(prior.rows)} → {len(sample.rows)} rows)")
        if fresh.connected:
            self.last_connected[fresh.id] = fresh

    def update_propagation(self, sampled: dict[str, tuple[float, str]] | None = None) -> None:
        sampled = sampled or {}
        primary = self.servers[0]
        if primary.connected and primary.innodb_lsn is not None:
            current_lsn = primary.innodb_lsn
            if self.primary_lsn is not None and current_lsn > self.primary_lsn:
                started, sampled_at = sampled.get(primary.id, (time.monotonic(), now()))
                for target in self.servers[1:]:
                    self.propagation_rows.append({
                        "primary_lsn": current_lsn,
                        "primary_checkpoint": primary.innodb_lsn_checkpoint,
                        "primary_seen_at": sampled_at,
                        "target_id": target.id,
                        "target_name": target.name,
                        "target_lsn": target.innodb_lsn,
                        "target_checkpoint": target.innodb_lsn_checkpoint,
                        "target_seen_at": "",
                        "latency_seconds": None,
                        "status": "waiting",
                        "started_monotonic": started,
                    })
                if len(self.propagation_rows) > PROPAGATION_LIMIT:
                    del self.propagation_rows[:-PROPAGATION_LIMIT]
            elif self.primary_lsn is not None and current_lsn < self.primary_lsn:
                for row in self.propagation_rows:
                    if row["status"] == "waiting":
                        row["status"] = "primary LSN reset"
            self.primary_lsn = current_lsn

        targets = {server.id: server for server in self.servers[1:]}
        for row in self.propagation_rows:
            if row["status"] != "waiting":
                continue
            target = targets[row["target_id"]]
            row["target_lsn"] = target.innodb_lsn
            row["target_checkpoint"] = target.innodb_lsn_checkpoint
            if target.connected and target.innodb_lsn is not None and target.innodb_lsn >= row["primary_lsn"]:
                finished, observed_at = sampled.get(target.id, (time.monotonic(), now()))
                row["status"] = "observed"
                row["target_seen_at"] = observed_at
                row["latency_seconds"] = round(max(0, finished - row["started_monotonic"]), 3)

    def run(self) -> None:
        with ThreadPoolExecutor(max_workers=min(len(self.servers), 16)) as pool:
            while not self.stop_event.is_set():
                began = time.monotonic()
                with self.lock:
                    originals = list(self.servers)
                futures = {pool.submit(self.query, server): server.id for server in originals}
                updates = {}
                sampled = {}
                for future in as_completed(futures):
                    server_id = futures[future]
                    updates[server_id] = future.result()
                    sampled[server_id] = (time.monotonic(), now())
                with self.lock:
                    for index, old in enumerate(self.servers):
                        fresh = updates[old.id]
                        self.evaluate(old, fresh)
                        self.servers[index] = fresh
                    self.update_propagation(sampled)
                self.stop_event.wait(max(0, self.config["interval"] - (time.monotonic() - began)))

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            servers = [asdict(server) for server in self.servers]
            for server in servers:
                for field_name in ("innodb_lsn", "innodb_lsn_checkpoint"):
                    if server[field_name] is not None:
                        server[field_name] = str(server[field_name])
            propagation_rows = [{key: value for key, value in row.items() if key != "started_monotonic"}
                                for row in self.propagation_rows]
            for row in propagation_rows:
                for field_name in ("primary_lsn", "primary_checkpoint", "target_lsn", "target_checkpoint"):
                    if row[field_name] is not None:
                        row[field_name] = str(row[field_name])
            return {"running": bool(self.thread and self.thread.is_alive() and not self.stop_event.is_set()),
                    "servers": servers,
                    "events": [asdict(e) for e in self.events], "row_limit": ROW_LIMIT,
                    "propagation_rows": propagation_rows,
                    "selected_tables": self.config["tables"]}


PAGE = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MySQL multi-server observer</title>
<style>
:root{font:14px system-ui,sans-serif;color:#182536;background:#f3f6fa}*{box-sizing:border-box}body{margin:0}header{padding:1rem 1.4rem;background:#17283d;color:white}h1{font-size:1.35rem;margin:0 0 .3rem}h2{font-size:1.1rem;margin:.1rem 0 .8rem}h3{font-size:.95rem;margin:.8rem 0 .4rem}p{margin:.3rem 0}main{padding:1rem;max-width:1800px;margin:auto}.config,.panel,.server{background:white;border:1px solid #d6e0ea;border-radius:8px;padding:1rem}.config{margin-bottom:1rem}.formgrid{display:flex;flex-wrap:wrap;gap:.55rem;align-items:end}.field{display:flex;flex-direction:column;gap:.2rem}.field label{font-size:.8rem;color:#526175}.field input{padding:.5rem;border:1px solid #bac8d8;border-radius:5px;min-width:115px}.serverinput{display:flex;gap:.5rem;align-items:center;margin:.5rem 0}.serverinput input{padding:.5rem;border:1px solid #bac8d8;border-radius:5px;min-width:0}.serverinput .name{width:170px}.serverinput .host{width:240px}.serverinput .port{width:90px}button{padding:.55rem .8rem;border:1px solid #a9b9ca;border-radius:5px;background:#fff;cursor:pointer}button.primary{background:#1459a4;color:white;border-color:#1459a4}button:disabled{opacity:.5;cursor:default}.actions{display:flex;gap:.5rem;align-items:center;margin-top:.8rem}.muted{color:#637287}.error{color:#a82020}.ok{color:#087443}.layout{display:grid;grid-template-columns:minmax(260px,320px) minmax(0,1fr);gap:1rem;min-height:500px}.left{display:flex;flex-direction:column;gap:.7rem}.right{display:grid;grid-template-rows:minmax(230px,40vh) minmax(320px,1fr);gap:1rem;min-width:0}.panel{min-width:0;overflow:auto}.server{padding:.8rem}.server b{display:block;margin-bottom:.25rem}.server div{margin:.18rem 0}.eventtable,.datagrid{border-collapse:collapse;width:100%;font-size:.82rem}.eventtable th,.eventtable td,.datagrid th,.datagrid td{border-bottom:1px solid #e5e9ef;padding:.4rem;text-align:left;vertical-align:top;white-space:nowrap}.eventtable th,.datagrid th{position:sticky;top:0;background:#eef3f8}.eventtable td:last-child{white-space:normal}.tablebox{margin:0 0 1rem;border:1px solid #d7e1eb;border-radius:6px;overflow:auto}.tablebox h3{position:sticky;left:0;padding:.6rem;margin:0;background:#f5f8fb}.tablepick{max-height:160px;overflow:auto;border:1px solid #d6e0ea;border-radius:5px;padding:.45rem;display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:.2rem}.tablepick label{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.tablepick input{vertical-align:middle}.toolbar{display:flex;gap:.5rem;align-items:center;margin:.5rem 0}small{color:#637287}@media(max-width:800px){.layout{grid-template-columns:1fr}.right{grid-template-rows:minmax(230px,40vh) minmax(320px,1fr)}.serverinput{flex-wrap:wrap}}
.server,.tablebox{border-color:hsl(var(--server-hue) 55% 68%);border-left:5px solid hsl(var(--server-hue) 65% 43%)}
.server{background:hsl(var(--server-hue) 75% 96%)}
.tablebox{background:hsl(var(--server-hue) 70% 99%)}
.tablebox h3,.tablebox .datagrid th{background:hsl(var(--server-hue) 70% 92%)}
.tablebox .datagrid tbody tr:nth-child(even){background:hsl(var(--server-hue) 70% 96%)}
.propagation{margin-top:1rem}
.propagation table{width:100%;border-collapse:collapse;font-size:.85rem}
.propagation th,.propagation td{padding:.45rem;border-bottom:1px solid #e5e9ef;text-align:left;white-space:nowrap}
.propagation th{background:#eef3f8;position:sticky;top:0}
</style></head><body><header><h1>MySQL multi-server observer</h1><p>Read-only observation of server status and performance_schema tables.</p></header><main>
<section class="config"><h2>Observer setup</h2><div id="serverInputs"></div><button type="button" id="addServer">Add DB server</button><div class="formgrid" style="margin-top:.7rem"><div class="field"><label for="user">MySQL user</label><input id="user" autocomplete="username"></div><div class="field"><label for="password">Password</label><input id="password" type="password" autocomplete="current-password"></div><div class="field"><label for="interval">Sample interval (seconds)</label><input id="interval" type="number" min="0.1" step="0.1" value="1"></div></div><h3>performance_schema tables</h3><div class="toolbar"><button type="button" id="discover">Load tables</button><span class="muted">Uses the first reachable server and the credentials above.</span></div><div id="tablepick" class="tablepick"><span class="muted">Load tables to select what to monitor.</span></div><div class="actions"><button type="button" id="start" class="primary">Start observe</button><button type="button" id="stop">Stop observer</button><span id="run" class="muted">Observer stopped</span></div><p id="message" role="status"></p></section>
<div class="layout"><aside class="left" id="servers"><section class="panel muted">Server status appears here after Start observe.</section></aside><div class="right"><section class="panel"><h2>Events <small id="eventnote"></small></h2><table class="eventtable"><thead><tr><th>UTC time</th><th>Elapsed</th><th>Server</th><th>Event</th><th>Detail</th></tr></thead><tbody id="events"></tbody></table></section><section class="panel"><h2>Selected table content <small id="limitnote"></small></h2><div id="contents" class="muted">Choose tables and start observing.</div></section></div></div>
<section class="panel propagation"><h2>LSN propagation <small>First server is primary; delay is measured from sampled LSN values.</small></h2><table><thead><tr><th>Primary observed (UTC)</th><th>Primary LSN</th><th>Primary checkpoint</th><th>Target server</th><th>Target LSN</th><th>Target checkpoint</th><th>Target observed (UTC)</th><th>Delay</th><th>Status</th></tr></thead><tbody id="propagation"><tr><td colspan="9" class="muted">Start observing at least two servers.</td></tr></tbody></table></section>
</main><script>
const $=s=>document.querySelector(s),esc=s=>String(s??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function addServer(name='',host='',port=3306){const row=document.createElement('div');row.className='serverinput';row.innerHTML=`<input class="name" placeholder="Display name" aria-label="Display name"><input class="host" placeholder="DB hostname or IP" aria-label="DB hostname or IP"><input class="port" type="number" min="1" max="65535" aria-label="MySQL port"><button type="button" aria-label="Remove server">Remove</button>`;row.querySelector('.name').value=name;row.querySelector('.host').value=host;row.querySelector('.port').value=port;row.querySelector('button').onclick=()=>row.remove();$('#serverInputs').append(row)}
addServer('Server 1');addServer('Server 2');$('#addServer').onclick=()=>addServer(`Server ${$('#serverInputs').children.length+1}`);
function inputs(){return [...document.querySelectorAll('.serverinput')].map(row=>({name:row.querySelector('.name').value.trim(),host:row.querySelector('.host').value.trim(),port:Number(row.querySelector('.port').value)}))}
async function api(path,options){const response=await fetch(path,options);const data=await response.json();if(!response.ok)throw Error(data.error||`HTTP ${response.status}`);return data}
function message(value,bad=false){$('#message').textContent=value;$('#message').className=bad?'error':'ok'}
$('#discover').onclick=async()=>{try{message('Loading tables…');const candidates=inputs().filter(s=>s.host);if(!candidates.length)throw Error('Enter a DB host first.');let result;let last;for(const s of candidates){try{result=await api('/api/tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({...s,user:$('#user').value,password:$('#password').value})});break}catch(error){last=error}}if(!result)throw last;$('#tablepick').innerHTML=result.tables.map(t=>`<label><input type="checkbox" value="${esc(t)}"> ${esc(t)}</label>`).join('')||'<span class="muted">No tables found.</span>';message(`Loaded ${result.tables.length} tables from ${result.host}.`)}catch(error){message(error.message,true)}};
$('#start').onclick=async()=>{try{const servers=inputs();const tables=[...document.querySelectorAll('#tablepick input:checked')].map(x=>x.value);await api('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({servers,tables,user:$('#user').value,password:$('#password').value,interval:Number($('#interval').value)})});message('Observer started.');refresh()}catch(error){message(error.message,true)}};
$('#stop').onclick=async()=>{try{await api('/api/stop',{method:'POST'});message('Observer stopped.');refresh()}catch(error){message(error.message,true)}};
function uptime(n){if(n==null)return '—';const d=Math.floor(n/86400),h=Math.floor(n%86400/3600),m=Math.floor(n%3600/60),s=Math.floor(n%60);return (d?`${d}d `:'')+[h,m,s].map(x=>String(x).padStart(2,'0')).join(':')}
const serverHues=[210,25,135,275,45,180,320,90,235,10,155,295,60,195,345,115];
function serverStyle(s){return `style="--server-hue:${serverHues[(Number(s.id.slice(1))-1)%serverHues.length]??210}"`}
function serverCard(s){return `<section class="server" ${serverStyle(s)}><b>${esc(s.name)}${s.id==='s1'?' (Primary)':''}</b><div>${esc(s.host)}:${esc(s.port)}</div><div class="${s.connected?'ok':'error'}">${s.connected?'CONNECTED':'NO CONNECTION'}</div><div>Hostname: ${esc(s.hostname)}</div><div>Uptime: ${uptime(s.uptime_seconds)}</div><div>Read only: ${s.read_only==null?'—':s.read_only?'YES':'NO (RW)'}</div><div>GTID executed: <span style="overflow-wrap:anywhere">${s.gtid_error?`<span class="error">${esc(s.gtid_error)}</span>`:esc(s.gtid_executed||'(empty)')}</span></div><div>Server UUID: ${esc(s.server_uuid||'—')}</div><div>InnoDB LSN: ${s.log_status_error?`<span class="error">${esc(s.log_status_error)}</span>`:esc(s.innodb_lsn??'—')}</div><div>InnoDB LSN checkpoint: ${esc(s.innodb_lsn_checkpoint??'—')}</div><div>Replication: ${esc(s.replication)}</div>${s.error?`<div class="error">${esc(s.error)}</div>`:''}<small>Last sample: ${esc(s.observed_at)}</small></section>`}
function tableBlock(s,name,t){let head=t.columns.map(c=>`<th>${esc(c)}</th>`).join('');let body=t.rows.map(row=>`<tr>${row.map(v=>`<td>${esc(v)}</td>`).join('')}</tr>`).join('');return `<div class="tablebox" ${serverStyle(s)}><h3>${esc(s.name)} · ${esc(name)} <small>(${t.rows.length} sampled rows; ${esc(t.observed_at)})</small></h3>${t.error?`<p class="error">${esc(t.error)}</p>`:t.rows.length?`<table class="datagrid"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`:'<p class="muted">No rows in sample.</p>'}</div>`}
function propagationRow(r){return `<tr><td>${esc(r.primary_seen_at)}</td><td>${esc(r.primary_lsn)}</td><td>${esc(r.primary_checkpoint)}</td><td>${esc(r.target_name)}</td><td>${esc(r.target_lsn)}</td><td>${esc(r.target_checkpoint)}</td><td>${esc(r.target_seen_at)}</td><td>${r.latency_seconds==null?'—':`${Number(r.latency_seconds).toFixed(3)} s`}</td><td>${esc(r.status)}</td></tr>`}
async function refresh(){try{const data=await api('/api/status');$('#run').textContent=data.running?'Observer running':'Observer stopped';if(!data.servers.length)return;$('#servers').innerHTML=data.servers.map(serverCard).join('');$('#events').innerHTML=[...data.events].reverse().map(e=>`<tr><td>${esc(e.timestamp_utc)}</td><td>${e.elapsed_seconds.toFixed(3)} s</td><td>${esc(e.server)}</td><td>${esc(e.event)}</td><td>${esc(e.detail)}</td></tr>`).join('');$('#eventnote').textContent=`${data.events.length} recent events` ;$('#limitnote').textContent=`first ${data.row_limit} rows per table`;$('#contents').innerHTML=data.servers.flatMap(s=>data.selected_tables.map(name=>tableBlock(s,name,s.tables[name]||{columns:[],rows:[],error:'Waiting for first sample',observed_at:''}))).join('')||'<span class="muted">No tables selected.</span>';$('#propagation').innerHTML=data.propagation_rows.slice(-100).reverse().map(propagationRow).join('')||'<tr><td colspan="9" class="muted">Waiting for the primary LSN to change.</td></tr>'}catch(error){message(error.message,true)}}
setInterval(refresh,1000);refresh();
</script></body></html>'''


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
        if not isinstance(servers, list) or not 1 <= len(servers) <= 16 or not isinstance(tables, list) or not 1 <= len(tables) <= 8:
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
        return jsonify(error="Enter 1–16 unique DB servers, 1–8 tables, a MySQL user, and a 0.1–60 second interval."), 400
    with app_lock:
        if monitor and monitor.thread and monitor.thread.is_alive() and not monitor.stop_event.is_set():
            return jsonify(error="Stop the current observer before starting another."), 409
        monitor = MultiMonitor({"servers": clean_servers, "tables": tables, "user": user,
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
                                                     "selected_tables": []})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5051)
    args = parser.parse_args()
    app.run(host="127.0.0.1", port=args.port, debug=False)
