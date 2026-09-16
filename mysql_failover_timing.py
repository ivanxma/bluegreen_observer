#!/usr/bin/env python3
"""Local web dashboard for timing an externally managed MySQL switchover.

Install: python -m pip install -r requirements.txt
Run:     python mysql_failover_timing.py
Open:    http://127.0.0.1:5050

This app only observes two MySQL servers.  It never executes a switchover.
Clicking "Start switchover timing" records the external-operation start time.
"""
from __future__ import annotations

import argparse
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)
state_lock = threading.Lock()
monitor: "DualMonitor | None" = None


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class ServerStatus:
    ip: str
    connected: bool = False
    hostname: str = "—"
    uptime_seconds: int | None = None
    uptime_error: str = ""
    read_only: bool | None = None
    replication: str = "Unknown / not available"
    replication_channels: dict[str, str] = field(default_factory=dict)
    replication_available: bool = False
    error: str = ""
    observed_at: str = ""


@dataclass
class Event:
    timestamp_utc: str
    seconds_from_switch: float | None
    server: str
    event: str
    detail: str = ""


class DualMonitor:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.status = {"primary": ServerStatus(config["primary_ip"]), "target": ServerStatus(config["target_ip"])}
        self.events: list[Event] = []
        self.switch_started_at: float | None = None
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.initial = {"primary": None, "target": None}
        self.milestones: dict[str, float] = {}

    def switch_elapsed(self) -> float | None:
        return None if self.switch_started_at is None else time.monotonic() - self.switch_started_at

    def log(self, server: str, event: str, detail: str = "") -> None:
        self.events.append(Event(iso_now(), self.switch_elapsed(), server, event, detail))

    def query(self, ip: str) -> ServerStatus:
        try:
            import mysql.connector
        except ImportError:
            return ServerStatus(ip, error="mysql-connector-python missing; install requirements.txt", observed_at=iso_now())
        try:
            con = mysql.connector.connect(host=ip, port=self.config["port"], user=self.config["user"],
                password=self.config["password"], database=self.config["database"] or None,
                connection_timeout=self.config["connect_timeout"], read_timeout=self.config["read_timeout"],
                write_timeout=self.config["read_timeout"], autocommit=True)
            try:
                with con.cursor() as cur:
                    cur.execute("SELECT @@hostname, @@global.read_only, COALESCE(@@global.super_read_only, 0)")
                    hostname, ro, super_ro = cur.fetchone()
                    # Uptime is a standard global status counter, so it remains
                    # useful even where replication-status access is restricted.
                    uptime_seconds: int | None = None
                    uptime_error = ""
                    try:
                        cur.execute("SHOW GLOBAL STATUS LIKE 'Uptime'")
                        uptime_row = cur.fetchone()
                        if uptime_row:
                            uptime_seconds = int(uptime_row[1])
                        else:
                            uptime_error = "The server did not return its Uptime status value."
                    except Exception as exc:
                        uptime_error = f"{type(exc).__name__}: {exc}"
                    # MySQL 8+ exposes channels here.  If permissions/version do not
                    # permit it, the dashboard reports that fact without failing.
                    channels_by_name: dict[str, str] = {}
                    replication_available = False
                    try:
                        cur.execute("SELECT CHANNEL_NAME, SERVICE_STATE FROM performance_schema.replication_connection_status")
                        channels = cur.fetchall()
                        channels_by_name = {str(name or "default"): str(status) for name, status in channels}
                        replication_available = True
                        replication = ", ".join(f"{name}: {status}" for name, status in channels_by_name.items()) or "No replication channels"
                    except Exception:
                        try:
                            try:
                                cur.execute("SHOW REPLICA STATUS")
                            except Exception:
                                cur.execute("SHOW SLAVE STATUS")
                            slave = cur.fetchone()
                            if slave:
                                values = dict(zip(cur.column_names, slave))
                                channel = str(values.get("Channel_Name") or "default")
                                io_state = str(values.get("Replica_IO_Running", values.get("Slave_IO_Running", "Unknown")))
                                sql_state = str(values.get("Replica_SQL_Running", values.get("Slave_SQL_Running", "Unknown")))
                                channels_by_name = {channel: f"IO={io_state}; SQL={sql_state}"}
                            replication_available = True
                            replication = ", ".join(f"{name}: {status}" for name, status in channels_by_name.items()) or "No replication channels"
                        except Exception:
                            replication = "Not available (version or privileges)"
                    return ServerStatus(ip=ip, connected=True, hostname=str(hostname), uptime_seconds=uptime_seconds,
                        uptime_error=uptime_error,
                        read_only=bool(ro or super_ro),
                        replication=replication, replication_channels=channels_by_name,
                        replication_available=replication_available, observed_at=iso_now())
            finally:
                con.close()
        except Exception as exc:
            return ServerStatus(ip, False, error=f"{type(exc).__name__}: {exc}", observed_at=iso_now())

    def evaluate(self, role: str, fresh: ServerStatus) -> None:
        old = self.status[role]
        if self.initial[role] is None:
            self.initial[role] = fresh
            self.log(role, "initial_status", f"connected={fresh.connected}; hostname={fresh.hostname}; readonly={fresh.read_only}")
        elif old.connected and not fresh.connected:
            self.log(role, "connection_lost", fresh.error)
            if self.switch_started_at is not None:
                self.milestones.setdefault(f"{role}_connection_lost", self.switch_elapsed() or 0)
        elif not old.connected and fresh.connected:
            self.log(role, "connection_restored", f"hostname={fresh.hostname}")
            if self.switch_started_at is not None:
                self.milestones.setdefault(f"{role}_connection_restored", self.switch_elapsed() or 0)
        if old.connected and fresh.connected and old.hostname != "—" and old.hostname != fresh.hostname:
            self.log(role, "hostname_changed", f"{old.hostname} → {fresh.hostname}")
            if self.switch_started_at is not None:
                self.milestones.setdefault(f"{role}_hostname_changed", self.switch_elapsed() or 0)
        if old.read_only is True and fresh.read_only is False:
            self.log(role, "read_write_enabled", f"hostname={fresh.hostname}")
            if self.switch_started_at is not None:
                self.milestones.setdefault(f"{role}_read_write", self.switch_elapsed() or 0)
        if old.read_only is False and fresh.read_only is True:
            self.log(role, "read_only_enabled", f"hostname={fresh.hostname}")
        if old.connected and fresh.connected and old.replication_available and fresh.replication_available:
            for channel, prior_state in old.replication_channels.items():
                if channel not in fresh.replication_channels:
                    self.log(role, "replication_channel_deleted", f"channel={channel}; last_state={prior_state}")
                    if self.switch_started_at is not None:
                        self.milestones.setdefault(f"{role}_channel_deleted:{channel}", self.switch_elapsed() or 0)
            for channel, channel_state in fresh.replication_channels.items():
                if channel not in old.replication_channels:
                    self.log(role, "replication_channel_created", f"channel={channel}; state={channel_state}")
                elif old.replication_channels[channel] != channel_state:
                    self.log(role, "replication_channel_state_changed", f"channel={channel}; {old.replication_channels[channel]} → {channel_state}")
        self.status[role] = fresh

    def run(self) -> None:
        while not self.stop_event.is_set():
            began = time.monotonic()
            for role in ("primary", "target"):
                self.evaluate(role, self.query(self.status[role].ip))
            self.stop_event.wait(max(0, self.config["interval"] - (time.monotonic() - began)))

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def mark_switch_started(self) -> None:
        if self.switch_started_at is None:
            self.switch_started_at = time.monotonic()
            self.milestones = {}
            self.log("operator", "switchover_timing_started", "External switchover started")

    def snapshot(self) -> dict[str, Any]:
        with state_lock:
            return {"running": bool(self.thread and self.thread.is_alive()), "switch_started": self.switch_started_at is not None,
                "servers": {name: asdict(value) for name, value in self.status.items()}, "events": [asdict(x) for x in self.events],
                "timings": self.milestones}


PAGE = '''<!doctype html><html><head><meta charset="utf-8"><title>MySQL Switchover Timer</title><style>
body{font:15px system-ui;margin:2rem;max-width:1120px;color:#172033}input{padding:.5rem;margin:.2rem;width:145px}button{padding:.55rem .9rem;margin:.2rem;font-weight:600}.danger{background:#b42318;color:#fff;border:0}.cards{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin:1rem 0}.card{border:1px solid #c9d2df;border-radius:9px;padding:1rem}.ok{color:#087443}.bad{color:#b42318}.metric{background:#eef3fa;border-radius:5px;padding:.6rem;margin:.3rem 0}table{width:100%;border-collapse:collapse;font-size:.86rem}th,td{padding:.45rem;border-bottom:1px solid #ddd;text-align:left}</style></head><body>
<h1>MySQL switchover observer</h1><p>Observes two database IPs. The switchover itself is managed externally.</p>
<form id="config"><input name="primary_ip" placeholder="Current primary IP" required><input name="target_ip" placeholder="Target DB IP" required><input name="user" placeholder="MySQL user" required><input name="password" type="password" placeholder="Password" required><input name="database" placeholder="Database optional"><input name="port" type="number" value="3306"><input name="interval" type="number" step=".05" value="0.25"><button>Start observer</button></form>
<p><button class="danger" onclick="mark()">Start switchover timing</button> <button onclick="stop()">Stop observer</button> <span id="run"></span></p><div class="cards" id="servers"></div><h2>Timings after button click</h2><div id="timings"></div><h2>Events</h2><table><thead><tr><th>UTC</th><th>After switch</th><th>Server</th><th>Event</th><th>Detail</th></tr></thead><tbody id="events"></tbody></table>
<script>const esc=s=>String(s??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));const sec=x=>x==null?'—':Number(x).toFixed(3)+' s';const uptime=x=>{if(x==null)return '—';let n=Math.max(0,Number(x)),d=Math.floor(n/86400),h=Math.floor(n%86400/3600),m=Math.floor(n%3600/60),s=Math.floor(n%60);return (d?d+'d ':'')+String(h).padStart(2,'0')+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0')};async function api(u,o){let r=await fetch(u,o);let j=await r.json();if(!r.ok)alert(j.error||'Request failed');return j}document.querySelector('#config').onsubmit=async e=>{e.preventDefault();let d=Object.fromEntries(new FormData(e.target));await api('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});refresh()};async function mark(){await api('/api/mark-switch',{method:'POST'});refresh()}async function stop(){await api('/api/stop',{method:'POST'});refresh()}function card(title,s){return `<section class="card"><h2>${title}</h2><div><b>IP:</b> ${esc(s.ip)}</div><div class="${s.connected?'ok':'bad'}"><b>Status:</b> ${s.connected?'CONNECTED':'NO CONNECTION'}</div><div><b>Hostname:</b> ${esc(s.hostname)}</div><div><b>Uptime:</b> ${esc(uptime(s.uptime_seconds))}</div><div><b>Read-only:</b> ${s.read_only==null?'—':s.read_only?'YES':'NO (RW)'}</div><div><b>Replication channels:</b> ${esc(s.replication)}</div>${s.error?`<div class="bad">${esc(s.error)}</div>`:''}<small>Last sample: ${esc(s.observed_at)}</small></section>`}function label(k){let labels={primary_connection_lost:'Primary connection lost',primary_connection_restored:'Primary connection restored',primary_hostname_changed:'Primary hostname changed',primary_read_write:'Primary writable',target_connection_lost:'Target connection lost',target_connection_restored:'Target connection restored',target_hostname_changed:'Target hostname changed',target_read_write:'Target writable'};return labels[k]||k.replace('_channel_deleted:',' replication channel deleted: ').replaceAll('_',' ')}async function refresh(){let s=await api('/api/status');document.querySelector('#run').textContent=s.running?'Observer running':'Observer stopped';document.querySelector('#servers').innerHTML=card('Current primary',s.servers.primary)+card('Target DB',s.servers.target);document.querySelector('#timings').innerHTML=s.switch_started?Object.entries(s.timings).map(([k,v])=>`<div class="metric"><b>${esc(label(k))}:</b> ${sec(v)}</div>`).join('')||'<div class="metric">Waiting for switchover events…</div>':'Click “Start switchover timing” when the external switchover begins.';document.querySelector('#events').innerHTML=s.events.map(e=>`<tr><td>${esc(e.timestamp_utc)}</td><td>${sec(e.seconds_from_switch)}</td><td>${esc(e.server)}</td><td>${esc(e.event)}</td><td>${esc(e.detail)}</td></tr>`).join('')}setInterval(refresh,500);refresh();</script></body></html>'''

@app.get("/")
def index(): return PAGE

@app.post("/api/start")
def start():
    global monitor
    p = request.get_json(silent=True) or {}
    try:
        cfg = {"primary_ip": str(p["primary_ip"]), "target_ip": str(p["target_ip"]), "user": str(p["user"]), "password": str(p["password"]), "database": str(p.get("database") or ""), "port": int(p.get("port") or 3306), "interval": float(p.get("interval") or .25), "connect_timeout": 2, "read_timeout": 2}
        if not all(cfg[x] for x in ("primary_ip", "target_ip", "user", "password")) or cfg["interval"] <= 0: raise ValueError
    except (KeyError, ValueError, TypeError): return jsonify(error="Enter both IPs, user, password, and a positive sample interval."), 400
    with state_lock:
        if monitor and monitor.thread and monitor.thread.is_alive(): return jsonify(error="Stop the current observer before starting another."), 409
        monitor = DualMonitor(cfg); monitor.start()
    return jsonify(ok=True)

@app.post("/api/mark-switch")
def mark_switch():
    if not monitor: return jsonify(error="Start the observer first."), 409
    monitor.mark_switch_started(); return jsonify(ok=True)

@app.post("/api/stop")
def stop():
    if monitor: monitor.stop_event.set()
    return jsonify(ok=True)

@app.get("/api/status")
def status(): return jsonify(monitor.snapshot() if monitor else {"running": False, "switch_started": False, "servers": {"primary": asdict(ServerStatus("—")), "target": asdict(ServerStatus("—"))}, "events": [], "timings": {}})

if __name__ == "__main__":
    cli = argparse.ArgumentParser(); cli.add_argument("--port", type=int, default=5050); args = cli.parse_args()
    app.run(host="127.0.0.1", port=args.port, debug=False)
