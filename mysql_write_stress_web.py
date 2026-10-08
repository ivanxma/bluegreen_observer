#!/usr/bin/env python3
"""Local web control for the MySQL write and secondary LSN stress test."""

from __future__ import annotations

import argparse
import threading
from pathlib import Path

from flask import Flask, jsonify, render_template_string, request, send_file

from mysql_write_stress import execute, identifier


app = Flask(__name__)


def config_from_json(data: dict) -> argparse.Namespace:
    if not isinstance(data, dict):
        raise ValueError("Provide a JSON object")

    def required(name: str) -> str:
        value = str(data.get(name, "")).strip()
        if not value:
            raise ValueError(f"{name} is required")
        return value

    def whole(name: str, default: int, maximum: int) -> int:
        value = data.get(name, default)
        try:
            number = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be an integer") from exc
        if str(value).strip() != str(number) or not 1 <= number <= maximum:
            raise ValueError(f"{name} must be between 1 and {maximum}")
        return number

    def decimal(name: str, default: float, maximum: float) -> float:
        try:
            number = float(data.get(name, default))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be a number") from exc
        if not 0 < number <= maximum:
            raise ValueError(f"{name} must be greater than 0 and at most {maximum}")
        return number

    database = required("database")
    prefix = str(data.get("table_prefix", "stress_txn")).strip()
    try:
        identifier(database)
        identifier(prefix)
    except argparse.ArgumentTypeError as exc:
        raise ValueError(f"database and table_prefix: {exc}") from exc
    threads = whole("threads", 4, 64)
    batch_size = whole("batch_size", 100, 100000)
    row_bytes = whole("row_bytes", 8192, 16_777_215)
    if len(prefix) + len(str(threads)) + 1 > 64:
        raise ValueError("table_prefix is too long")
    if batch_size * row_bytes > 64 * 1024 * 1024:
        raise ValueError("batch_size × row_bytes must be at most 64 MiB")
    password = data.get("password")
    if not isinstance(password, str) or not password:
        raise ValueError("password is required")
    return argparse.Namespace(
        host=required("host"), port=whole("port", 3306, 65535),
        secondary_host=required("secondary_host"),
        secondary_port=whole("secondary_port", 3306, 65535),
        user=required("user"), password=password, database=database,
        threads=threads, batch_size=batch_size, duration=decimal("duration", 60, 86400),
        row_bytes=row_bytes, table_prefix=prefix,
        lag_interval=decimal("lag_interval", 1, 60),
        catchup_timeout=decimal("catchup_timeout", 60, 3600), lag_csv=None,
    )


class RunController:
    def __init__(self):
        self.lock = threading.Lock()
        self.stop_request = threading.Event()
        self.thread: threading.Thread | None = None
        self.status = self._empty_status()

    @staticmethod
    def _empty_status() -> dict:
        return {"state": "idle", "run_id": None, "tables": [], "transactions": 0,
                "rows": 0, "payload_bytes": 0, "sample": None, "summary": None,
                "error": "", "stop_requested": False, "started_at": None,
                "lag_csv": None}

    def snapshot(self) -> dict:
        with self.lock:
            return {**self.status, "tables": list(self.status["tables"]),
                    "sample": dict(self.status["sample"]) if self.status["sample"] else None}

    def start(self, args: argparse.Namespace) -> bool:
        with self.lock:
            if self.thread is not None and self.thread.is_alive():
                return False
            self.stop_request = threading.Event()
            self.status = self._empty_status()
            self.status["state"] = "starting"
            self.thread = threading.Thread(target=self._run, args=(args, self.stop_request), daemon=True)
            self.thread.start()
            return True

    def stop(self) -> bool:
        with self.lock:
            if self.thread is None or not self.thread.is_alive():
                return False
            self.stop_request.set()
            self.status["stop_requested"] = True
            return True

    def _progress(self, update: dict) -> None:
        with self.lock:
            if "commit" in update:
                self.status["transactions"] += 1
                self.status["rows"] += update["commit"]["rows"]
                self.status["payload_bytes"] += update["commit"]["payload_bytes"]
            elif "sample" in update:
                self.status["sample"] = update["sample"]
            else:
                self.status.update(update)

    def _run(self, args: argparse.Namespace, stop_request: threading.Event) -> None:
        try:
            summary = execute(args, stop_request, self._progress)
            with self.lock:
                self.status["summary"] = summary
                self.status["state"] = summary["state"]
                self.status["transactions"] = summary["transactions"]
                self.status["rows"] = summary["rows"]
                self.status["payload_bytes"] = summary["total_payload_bytes"]
        except Exception as exc:
            with self.lock:
                self.status["state"] = "failed"
                self.status["error"] = f"{type(exc).__name__}: {exc}"


controller = RunController()


@app.get("/")
def index():
    return render_template_string(PAGE)


@app.get("/api/status")
def status():
    return jsonify(controller.snapshot())


@app.post("/api/start")
def start():
    try:
        args = config_from_json(request.get_json(silent=True))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not controller.start(args):
        return jsonify({"error": "A test is already running"}), 409
    return jsonify({"ok": True}), 202


@app.post("/api/stop")
def stop():
    if not controller.stop():
        return jsonify({"error": "No test is running"}), 409
    return jsonify({"ok": True}), 202


@app.get("/api/lag.csv")
def lag_csv():
    path = controller.snapshot()["lag_csv"]
    if not path or not Path(path).is_file():
        return jsonify({"error": "Lag CSV is not available yet"}), 404
    return send_file(Path(path).resolve(), as_attachment=True, download_name=Path(path).name,
                     mimetype="text/csv")


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MySQL write stress</title><style>
:root{font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#172b45;background:#f3f6fa}
*{box-sizing:border-box}body{max-width:1160px;margin:0 auto;padding:28px 18px 60px}
h1{margin:0 0 4px;font-size:2rem}h2{font-size:1.1rem;margin:0 0 16px}p{line-height:1.45}
.sub{color:#5e7086;margin:0 0 25px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));gap:18px}
.panel{background:#fff;border:1px solid #dce4ee;border-radius:12px;padding:20px;box-shadow:0 3px 14px #132b4510}
.fields{display:grid;grid-template-columns:1fr 1fr;gap:12px}.full{grid-column:1/-1}
label{display:block;font-size:.86rem;font-weight:650;color:#425672}input{width:100%;margin-top:5px;padding:10px;border:1px solid #b9c7d7;border-radius:7px;font:inherit}
input:focus{outline:2px solid #2f74d0;outline-offset:1px}.actions{display:flex;gap:10px;margin:19px 0 0}
button{border:0;border-radius:7px;background:#1766c2;color:white;font:inherit;font-weight:650;padding:11px 18px;cursor:pointer}
button.stop{background:#ba4137}button:disabled{background:#9aaabc;cursor:default}
.status{display:inline-block;padding:5px 10px;border-radius:20px;background:#e7edf5;font-weight:650;text-transform:capitalize}
.status.running{background:#d6f4e5;color:#176342}.status.catching_up{background:#fff0c8;color:#784f00}
.status.failed{background:#ffe0dd;color:#9f2e25}.status.completed{background:#d6f4e5;color:#176342}
.muted{color:#63768c;font-size:.86rem}.error{color:#a7251d;font-weight:600;white-space:pre-wrap}
table{width:100%;border-collapse:collapse;font-size:.88rem;margin-top:10px}th,td{text-align:left;padding:8px;border-bottom:1px solid #e2e8f0}
th{color:#425672;background:#f3f7fb}.group-row th{background:#e8f0fa;color:#244a75;padding-top:11px}
.wide{margin-top:18px}.hidden{display:none}a{color:#1766c2}
</style></head><body>
<h1>MySQL write stress</h1><p class="sub">Create one InnoDB table per worker, commit batches, and observe secondary LSN and row visibility.</p>
<div class="grid"><section class="panel"><h2>Connection and workload</h2>
<form id="config"><div class="fields">
<label>Primary IP or host<input name="host" required placeholder="10.0.0.1"></label>
<label>Primary port<input name="port" type="number" min="1" max="65535" value="3306" required></label>
<label>Secondary IP or host<input name="secondary_host" required placeholder="10.0.0.2"></label>
<label>Secondary port<input name="secondary_port" type="number" min="1" max="65535" value="3306" required></label>
<label>Database<input name="database" required placeholder="loadtest"></label>
<label>MySQL user<input name="user" required></label>
<label class="full">Password<input name="password" type="password" required autocomplete="off"></label>
<label>Threads / tables<input name="threads" type="number" min="1" max="64" value="4" required></label>
<label>Rows per commit<input name="batch_size" type="number" min="1" value="100" required></label>
<label>Payload bytes per row<input name="row_bytes" type="number" min="1" value="8192" required></label>
<label>Duration (seconds)<input name="duration" type="number" min="0.1" step="any" value="60" required></label>
<label>Lag sample interval (seconds)<input name="lag_interval" type="number" min="0.1" step="any" value="1" required></label>
<label>Catch-up wait (seconds)<input name="catchup_timeout" type="number" min="0.1" step="any" value="60" required></label>
</div><div class="actions"><button id="start" type="submit">Start test</button><button id="stop" class="stop" type="button" disabled>Stop writes</button></div>
<p class="muted">Stop finishes the current transactions, then waits for the secondary to catch up or reach the timeout.</p><div id="form-error" class="error"></div></form></section>
<section class="panel"><h2>Run status</h2><span id="state" class="status">Idle</span><p id="run-id" class="muted"></p>
<table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody id="live-rows"></tbody></table><div id="run-error" class="error"></div>
<a id="download" class="hidden" href="/api/lag.csv">Download lag samples (CSV)</a></section></div>
<section class="panel wide"><h2>Final result</h2><div id="summary" class="muted">Results appear after the test stops and catch-up monitoring finishes.</div>
<div id="table-wrap" class="hidden"><table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody id="result-rows"></tbody></table>
<h2 style="margin-top:22px">Per-table writes</h2><table><thead><tr><th>Table</th><th>Commits</th><th>Rows</th><th>Error</th></tr></thead><tbody id="table-rows"></tbody></table></div>
<p class="muted">Row and payload rates use the batches still absent on the secondary when each primary LSN was sampled. Zero-backlog samples have no normalized value. Redo LSN comparisons require comparable LSNs on both servers; sampling limits precision. Payload size excludes row and index overhead.</p></section>
<script>
const $=id=>document.getElementById(id);const num=n=>Number(n||0).toLocaleString();
const bytes=n=>`${(Number(n||0)/1048576).toFixed(2)} MiB`;
const sec=n=>n==null?'—':`${Number(n).toFixed(3)} s`;
const fixed=(n,unit)=>n==null?'—':`${Number(n).toFixed(3)} ${unit}`;
async function api(path,options){const response=await fetch(path,options);const data=await response.json();if(!response.ok)throw Error(data.error||`HTTP ${response.status}`);return data}
function group(parent,title,entries){const heading=document.createElement('tr');heading.className='group-row';const header=document.createElement('th');header.colSpan=2;header.textContent=title;heading.append(header);parent.append(heading);
  for(const [label,value] of entries){const tr=document.createElement('tr');const th=document.createElement('th');th.scope='row';th.textContent=label;const td=document.createElement('td');td.textContent=value;tr.append(th,td);parent.append(tr)}}
function render(s){const active=['starting','running','catching_up','finishing'].includes(s.state);
  $('state').textContent=s.stop_requested&&s.state==='running'?'Stopping writes':s.state.replaceAll('_',' ');$('state').className='status '+s.state;
  $('start').disabled=active;$('stop').disabled=!active||s.stop_requested||s.state==='catching_up'||s.state==='finishing';
  $('run-id').textContent=s.run_id?`Run ID: ${s.run_id}`:'';
  const live=$('live-rows');live.replaceChildren();group(live,'Write totals',[["Committed rows",num(s.rows)],["Transactions",num(s.transactions)],["Payload committed",bytes(s.payload_bytes)]]);
  if(s.sample){group(live,'Secondary sample',[["Primary LSN",num(s.sample.primary_lsn)],["Secondary LSN",num(s.sample.secondary_lsn)],["LSN gap",num(s.sample.lsn_gap)],["Visible / committed batches",`${num(s.sample.visible_batches)} / ${num(s.sample.committed_batches)}`],["Outstanding batches",num(s.sample.outstanding_batches)],["Samples",num(s.sample.samples)]]);
    group(live,'Latest normalized arrival',[["By rows",fixed(s.sample.newly_matched_ms_per_1000_rows,'ms/1,000 rows')],["By payload",fixed(s.sample.newly_matched_ms_per_mib_payload,'ms/MiB')]])}
  $('run-error').textContent=s.error||'';$('download').classList.toggle('hidden',!s.lag_csv);
  const out=$('summary');const results=$('result-rows');results.replaceChildren();$('table-rows').replaceChildren();$('table-wrap').classList.toggle('hidden',!s.summary);
  if(!s.summary){out.textContent='Results appear after the test stops and catch-up monitoring finishes.';return}
  const r=s.summary;out.textContent='';
  group(results,'Workload',[["Stopped by",r.stop_reason==='manual'?'Manual stop':'Duration'],["Committed rows",num(r.rows)],["Transactions",num(r.transactions)],["Rows per commit",num(r.rows_per_commit)],["Payload per commit",bytes(r.payload_bytes_per_commit)],["Write duration",sec(r.write_seconds)],["Write rate",fixed(r.rows_per_second,'rows/s')]]);
  if(r.secondary){const v=r.secondary;
    group(results,'Secondary catch-up',[["Result",v.caught_up?'Observed':'Not observed before timeout'],["Outstanding batches at end",v.final_outstanding_batches==null?'—':num(v.final_outstanding_batches)],["LSN arrivals measured",num(v.lsn_arrivals)],["Maximum sampled LSN gap",num(v.max_sampled_lsn_gap)]]);
    group(results,'Raw LSN timing',[["Mean arrival",sec(v.mean_lsn_arrival_seconds)],["Maximum arrival",sec(v.max_lsn_arrival_seconds)],["Final primary LSN arrival",sec(v.final_lsn_arrival_seconds)]]);
    group(results,'Normalized by pending rows',[["Median catch-up",fixed(v.median_ms_per_1000_rows,'ms/1,000 rows')],["Final catch-up",fixed(v.final_ms_per_1000_rows,'ms/1,000 rows')],["Median observed rate",fixed(v.median_rows_per_second,'rows/s')],["Final observed rate",fixed(v.final_rows_per_second,'rows/s')]]);
    group(results,'Normalized by pending payload',[["Median catch-up",fixed(v.median_ms_per_mib_payload,'ms/MiB')],["Final catch-up",fixed(v.final_ms_per_mib_payload,'ms/MiB')],["Median observed rate",fixed(v.median_payload_mib_per_second,'MiB/s')],["Final observed rate",fixed(v.final_payload_mib_per_second,'MiB/s')]]);
    if(v.error)group(results,'Monitor error',[["Error",v.error]])}
  for(const t of r.tables){const tr=document.createElement('tr');for(const value of [t.table,num(t.batches),num(t.rows),t.error||'—']){const td=document.createElement('td');td.textContent=value;tr.append(td)}$('table-rows').append(tr)}
}
async function refresh(){try{render(await api('/api/status'))}catch(error){$('run-error').textContent=error.message}}
$('config').addEventListener('submit',async event=>{event.preventDefault();$('form-error').textContent='';
  const data=Object.fromEntries(new FormData(event.currentTarget).entries());try{await api('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});refresh()}catch(error){$('form-error').textContent=error.message}});
$('stop').addEventListener('click',async()=>{try{await api('/api/stop',{method:'POST'});refresh()}catch(error){$('form-error').textContent=error.message}});
refresh();setInterval(refresh,1000);
</script></body></html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="Web bind address (default: localhost)")
    parser.add_argument("--port", type=int, default=5053, help="Web port (default: 5053)")
    args = parser.parse_args()
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
