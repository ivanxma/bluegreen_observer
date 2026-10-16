#!/usr/bin/env python3
"""ElasticDBCluster dashboard with fresh and persistent MySQL connections.

Run: python elasticdb_cluster_observer.py (http://127.0.0.1:5052).
Shared server sampling, events, table reads and LSN tracking live in
mysql_multi_observer.py. Observation is read-only; the optional primary test client writes to its test table.
"""
from __future__ import annotations

import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, replace
from typing import Any

from flask import Flask, jsonify, request

from elasticdb_test_client import TestRun, config_from_json

from mysql_multi_observer import (
    IDENTIFIER, ROW_LIMIT, MultiMonitor, PAGE as MULTI_PAGE,
    ServerSample, connect, now, tables_for,
)

app = Flask(__name__)
app_lock = threading.Lock()
monitor: ElasticDBClusterMonitor | None = None
test_runs: dict[str, TestRun] = {}


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


PAGE = PAGE.replace('<main>', '<main><div class="tabs" role="tablist" aria-label="Cluster tools"><button id="observerTab" role="tab" class="active" aria-selected="true" aria-controls="observerView">Observer</button><button id="testTab" role="tab" aria-selected="false" aria-controls="testView" tabindex="-1">Test Client</button></div><div id="observerView" role="tabpanel" aria-labelledby="observerTab">')
PAGE = PAGE.replace('</main>', r'''</div><div id="testView" role="tabpanel" aria-labelledby="testTab" hidden>
<section class="panel"><h2>Test Client</h2><p>Both tests loop until their Stop button is clicked. Observer sampling continues independently in either tab.</p>
<p>Uses the MySQL user and password from Observer setup at Start. Configure the Primary VIP and read-only endpoint separately below.</p>
<p>Test 1 creates <code>elastic_observer_test</code> in your existing test database and keeps one counter row per worker per run. Requires CREATE, SELECT, INSERT and UPDATE privileges. Rows remain after Stop.</p>
<p>Test 2 keeps each session open for repeated <code>SELECT SLEEP(seconds)</code> queries. Stop interrupts the queries with a separate same-account connection.</p>
<p class="muted">Each test also attempts a new login and SELECT 1 every second. Interruption runs from the start of the first failed operation to a successful operation; query time and retry delays are included. This is sampled client impact, not an exact server outage. Failed COMMIT replies are counted as uncertain outcomes.</p>
<p id="testMessage" role="status"></p></section>
<div id="testCases"></div>
</div></main>''')
PAGE = PAGE.replace('</style>', '''
[hidden]{display:none!important}.tabs{display:flex;gap:.3rem;border-bottom:1px solid #bac8d8;margin-bottom:1rem}.tabs button{border-radius:6px 6px 0 0}.tabs button.active{background:white;border-bottom-color:white;font-weight:700;margin-bottom:-1px}.primary{background:linear-gradient(#c74634,#a52e20)!important;border-color:#a52e20!important}.primary:disabled{background:#aeb5bc!important;border-color:#aeb5bc!important}.testcase{margin-top:1rem}.testresults{max-height:400px;overflow:auto;margin-top:1rem}.testresults table td{white-space:normal;min-width:100px}.testresults table td:last-child{min-width:200px}
</style>''')
PAGE = PAGE.replace('</script>', r'''
const toolTabs=[$('#observerTab'),$('#testTab')];
function selectToolTab(index){toolTabs.forEach((tab,i)=>{tab.classList.toggle('active',i===index);tab.setAttribute('aria-selected',String(i===index));tab.tabIndex=i===index?0:-1;$('#'+tab.getAttribute('aria-controls')).hidden=i!==index})}
toolTabs.forEach((tab,i)=>{tab.onclick=()=>selectToolTab(i);tab.onkeydown=e=>{let index;if(e.key==='ArrowRight'||e.key==='ArrowLeft')index=1-i;else if(e.key==='Home')index=0;else if(e.key==='End')index=1;else return;e.preventDefault();selectToolTab(index);toolTabs[index].focus()}});
$('#testCases').innerHTML=['primary','readonly'].map(kind=>`<section class="panel testcase"><h2>${kind==='primary'?'Test 1 · Primary VIP transactions':'Test 2 · Read-only long queries'}</h2><div class="formgrid">
<div class="field"><label for="${kind}Host">${kind==='primary'?'Primary VIP':'Read-only node host'} *</label><input id="${kind}Host" required></div>
<div class="field"><label for="${kind}Port">Port *</label><input id="${kind}Port" type="number" min="1" max="65535" value="3306"></div>
<div class="field"><label for="${kind}Threads">Threads *</label><input id="${kind}Threads" type="number" min="1" max="64" value="1"></div>
${kind==='primary'?'<div class="field"><label for="primaryDatabase">Test database *</label><input id="primaryDatabase" placeholder="Existing database"></div>':'<div class="field"><label for="readonlySleep">Sleep timer (seconds) *</label><input id="readonlySleep" type="number" min="0.1" max="3600" step="0.1" value="10"></div>'}
<div class="field"><label for="${kind}Pause">Loop pause (seconds) *</label><input id="${kind}Pause" type="number" min="0.01" max="60" step="0.01" value="0.1"></div>
</div><div class="actions"><button id="${kind}Start" class="primary">${kind==='primary'?'Start test client':'Test 2 · Start'}</button><button id="${kind}Stop" disabled>Stop</button><span id="${kind}State">Idle</span></div><div class="testresults" id="${kind}Results"></div><div class="testresults"><h3>Workload events (UTC, latest 500)</h3><table class="eventtable"><thead><tr><th>UTC time</th><th>Session</th><th>Event</th><th>Detail</th></tr></thead><tbody id="${kind}Events"></tbody></table></div></section>`).join('');
function testMessage(text,bad=false){$('#testMessage').textContent=text;$('#testMessage').className=bad?'error':'ok'}
for(const kind of ['primary','readonly']){
 $('#'+kind+'Start').onclick=async()=>{const button=$('#'+kind+'Start');button.disabled=true;try{const data={host:$('#'+kind+'Host').value,port:Number($('#'+kind+'Port').value),threads:Number($('#'+kind+'Threads').value),pause:Number($('#'+kind+'Pause').value),user:$('#user').value,password:$('#password').value,database:$('#primaryDatabase').value,sleep:Number($('#readonlySleep').value)};await api('/api/tests/'+kind+'/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});testMessage('Test started. Observer monitoring continues.');await refreshTests()}catch(e){testMessage(e.message,true);button.disabled=false}};
 $('#'+kind+'Stop').onclick=async()=>{try{await api('/api/tests/'+kind+'/stop',{method:'POST'});await refreshTests()}catch(e){testMessage(e.message,true)}};
}
function renderTest(kind,run){const running=!!run?.running;$('#'+kind+'Start').disabled=running;$('#'+kind+'Stop').disabled=!running||run.stopping;$('#'+kind+'State').textContent=run?(run.stopping?'Stopping…':running?'Running':'Stopped')+' · '+run.host+':'+run.port:'Idle';if(!run)return;
const columns=[['Session',(s,k)=>k],['Status',s=>s.status],['Backend / connection',s=>`${s.hostname||'—'} / ${s.connection_id??'—'}`],['Completed',s=>s.successes],['Failures',s=>s.failures],['Connections / reconnects',(s,k)=>`${s.connections} / ${k==='fresh-login'?'—':Math.max(0,s.connections-1)}`],['Last / max operation (s)',s=>`${s.latency_seconds??'—'} / ${s.max_latency_seconds}`],['Interruption / max (s)',s=>`${s.outage_seconds??'—'}${s.outage_active?' (ongoing)':''} / ${s.max_outage_seconds}`],['Last success (UTC)',s=>s.last_success_at||'—'],['Last failure (UTC)',s=>s.last_failure_at||'—'],['Uncertain commits',s=>s.uncertain_commits],['Last error',s=>s.last_error||'—']];
$('#'+kind+'Results').innerHTML=`<p>Run started ${esc(run.started_at)}${run.stopped_at?' · Stopped '+esc(run.stopped_at):''}</p>${run.cancel_error?'<p class="error">Stop interruption error: '+esc(run.cancel_error)+'; waiting for the operation timeout.</p>':''}<table class="eventtable"><thead><tr>${columns.map(([label])=>'<th>'+label+'</th>').join('')}</tr></thead><tbody>${Object.entries(run.sessions).map(([key,s])=>'<tr>'+columns.map(([,value])=>'<td>'+esc(value(s,key))+'</td>').join('')+'</tr>').join('')}</tbody></table>`;
$('#'+kind+'Events').innerHTML=[...run.events].reverse().map(e=>`<tr><td>${esc(e.timestamp_utc)}</td><td>${esc(e.session)}</td><td>${esc(e.event)}</td><td>${esc(e.detail)}</td></tr>`).join('')}
let testsRefreshing=false;
async function refreshTests(){if(testsRefreshing)return;testsRefreshing=true;try{const runs=await api('/api/tests/status');for(const kind of ['primary','readonly'])renderTest(kind,runs[kind])}catch(e){testMessage(e.message,true)}finally{testsRefreshing=false}}
setInterval(refreshTests,1000);refreshTests();
</script>''')


@app.post('/api/tests/<kind>/start')
def start_test(kind):
    data = request.get_json(silent=True)
    try:
        if not isinstance(data, dict):
            raise ValueError('Enter test connection settings.')
        config = config_from_json(data, kind)
    except (ValueError, TypeError, OverflowError) as exc:
        return jsonify(error=str(exc)), 400
    with app_lock:
        current = test_runs.get(kind)
        if current and current.thread and current.thread.is_alive():
            return jsonify(error='This test is running or stopping.'), 409
        current = TestRun(kind, config)
        test_runs[kind] = current
        current.start()
    return jsonify(ok=True), 202


@app.post('/api/tests/<kind>/stop')
def stop_test(kind):
    with app_lock:
        current = test_runs.get(kind)
        if not current:
            return jsonify(error='No such test run.'), 404
        if not current.stop_event.is_set():
            current.stop()
    return jsonify(ok=True), 202


@app.get('/api/tests/status')
def test_status():
    with app_lock:
        runs = dict(test_runs)
    return jsonify({kind: run.snapshot() for kind, run in runs.items()})


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
