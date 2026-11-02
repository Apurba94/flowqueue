"""The durable queue, cron schedules and workflows, all stored in one SQLite file.

Delivery is at-least-once: a worker claims a job under a lease, and if it dies the lease
runs out and another worker takes the job. Handlers should therefore be safe to repeat.
"""
import json
import random
import sqlite3
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, queue TEXT NOT NULL, task TEXT NOT NULL, payload TEXT NOT NULL,
  status TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0, run_at REAL NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL, worker TEXT, lease_until REAL,
  created REAL NOT NULL, finished REAL, result TEXT, error TEXT, unique_key TEXT,
  flow TEXT, step TEXT);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (queue, status, priority DESC, run_at);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique ON jobs (unique_key) WHERE unique_key IS NOT NULL AND status IN ('ready', 'running');
CREATE TABLE IF NOT EXISTS schedules (name TEXT PRIMARY KEY, cron TEXT NOT NULL, queue TEXT NOT NULL, task TEXT NOT NULL, payload TEXT NOT NULL, next_run REAL NOT NULL, enabled INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS flows (id TEXT PRIMARY KEY, name TEXT NOT NULL, spec TEXT NOT NULL, status TEXT NOT NULL, created REAL NOT NULL, finished REAL);
CREATE TABLE IF NOT EXISTS steps (flow TEXT NOT NULL, step TEXT NOT NULL, status TEXT NOT NULL, result TEXT, PRIMARY KEY (flow, step));
"""


class QueueError(Exception):
    pass


class Retry(Exception):
    """Raise from a handler to retry after a chosen delay."""

    def __init__(self, delay):
        super().__init__(f"retry in {delay}s")
        self.delay = delay


class Skip(Exception):
    """Raise from a handler to fail at once, without using up retries."""


FIELDS = (("minute", 0, 59), ("hour", 0, 23), ("day", 1, 31), ("month", 1, 12), ("weekday", 0, 6))
NAMES = {"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
         "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


class Cron:
    """A five-field cron expression: minute hour day month weekday."""

    def __init__(self, expr):
        parts = expr.split()
        if len(parts) != 5:
            raise QueueError(f"cron needs five fields: {expr!r}")
        self.expr = expr
        self.sets = [self._field(p, lo, hi, name) for p, (name, lo, hi) in zip(parts, FIELDS)]
        self.day_any, self.week_any = parts[2] == "*", parts[4] == "*"

    @staticmethod
    def _value(text, name):
        v = NAMES.get(text.lower())
        if v is None:
            try:
                v = int(text)
            except ValueError:
                raise QueueError(f"bad value {text!r} in {name}") from None
        return 0 if name == "weekday" and v == 7 else v

    def _field(self, text, lo, hi, name):
        out = set()
        for part in text.split(","):
            body, _, step = part.partition("/")
            step = int(step) if step else 1
            if step < 1:
                raise QueueError(f"bad step in {name}")
            if body == "*":
                a, b = lo, hi
            elif "-" in body:
                a, b = (self._value(x, name) for x in body.split("-", 1))
            else:
                a = self._value(body, name)
                b = hi if "/" in part else a
            if a < lo or b > hi or a > b:
                raise QueueError(f"{name} out of range: {part}")
            out.update(range(a, b + 1, step))
        return out

    def matches(self, t):
        minute, hour, day, month, week = self.sets
        if t.minute not in minute or t.hour not in hour or t.month not in month:
            return False
        d, w = t.day in day, (t.isoweekday() % 7) in week
        if self.day_any or self.week_any:     # when both are limited, cron means either one
            return d and w
        return d or w

    def next_after(self, t):
        """The first matching minute strictly after t, skipping quickly through days and months."""
        t = t.replace(second=0, microsecond=0) + timedelta(minutes=1)
        limit = t + timedelta(days=366 * 8)
        while t < limit:
            if t.month not in self.sets[3]:
                t = (t.replace(day=1, hour=0, minute=0) + timedelta(days=32)).replace(day=1)
                continue
            d, w = t.day in self.sets[2], (t.isoweekday() % 7) in self.sets[4]
            ok = (d and w) if (self.day_any or self.week_any) else (d or w)
            if not ok:
                t = t.replace(hour=0, minute=0) + timedelta(days=1)
                continue
            if t.hour not in self.sets[1]:
                t = t.replace(minute=0) + timedelta(hours=1)
                continue
            if t.minute in self.sets[0]:
                return t
            t += timedelta(minutes=1)
        raise QueueError(f"cron never fires: {self.expr}")


class Queue:
    def __init__(self, path, clock=None, seed=None):
        self.path = path
        self.clock = clock or time.time
        self.rng = random.Random(seed)
        self.local = threading.local()
        self.conn.executescript(SCHEMA)

    @property
    def conn(self):
        c = getattr(self.local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            self.local.conn = c
        return c

    def close(self):
        c = getattr(self.local, "conn", None)
        if c:
            c.close()
            self.local.conn = None

    def enqueue(self, task, payload=None, queue="default", delay=0, run_at=None, priority=0, max_attempts=5, unique=None, flow=None, step=None):
        """Add a job and return its id. With `unique`, a duplicate of a live job returns that job's id."""
        now = self.clock()
        jid = uuid.uuid4().hex
        try:
            self.conn.execute(
                "INSERT INTO jobs (id, queue, task, payload, status, priority, run_at, max_attempts, created, unique_key, flow, step) VALUES (?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?, ?)",
                (jid, queue, task, json.dumps(payload), priority, run_at if run_at is not None else now + delay, max_attempts, now, unique, flow, step))
        except sqlite3.IntegrityError:
            row = self.conn.execute("SELECT id FROM jobs WHERE unique_key = ? AND status IN ('ready', 'running')", (unique,)).fetchone()
            return row["id"]
        return jid

    def claim(self, worker, queues=("default",), lease=60):
        """Take the next job, or return None. The transaction stops two workers taking the same one."""
        now = self.clock()
        marks = ",".join("?" * len(queues))
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(f"SELECT * FROM jobs WHERE status = 'ready' AND queue IN ({marks}) AND run_at <= ? ORDER BY priority DESC, run_at, created LIMIT 1", (*queues, now)).fetchone()
            if row is None:
                c.execute("COMMIT")
                return None
            c.execute("UPDATE jobs SET status = 'running', worker = ?, lease_until = ?, attempts = attempts + 1 WHERE id = ?", (worker, now + lease, row["id"]))
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise
        job = dict(row)
        job["payload"] = json.loads(job["payload"])
        job["attempts"] += 1
        return job

    def heartbeat(self, job_id, worker, lease=60):
        cur = self.conn.execute("UPDATE jobs SET lease_until = ? WHERE id = ? AND worker = ? AND status = 'running'", (self.clock() + lease, job_id, worker))
        return cur.rowcount == 1

    def _owned(self, job_id, worker):
        row = self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise QueueError(f"no such job: {job_id}")
        return row if row["status"] == "running" and row["worker"] == worker else None

    def complete(self, job_id, worker, result=None):
        """Finish a job. A worker that lost its lease is ignored, so a late finish changes nothing."""
        row = self._owned(job_id, worker)
        if row is None:
            return False
        self.conn.execute("UPDATE jobs SET status = 'done', finished = ?, result = ?, worker = NULL, lease_until = NULL WHERE id = ?", (self.clock(), json.dumps(result), job_id))
        if row["flow"]:
            self._step_done(row["flow"], row["step"], result)
        return True

    def backoff(self, attempts, base=2.0, cap=3600):
        """Exponential delay with jitter, so failed jobs do not all return at once."""
        return min(cap, base ** attempts) * (0.5 + self.rng.random() / 2)

    def fail(self, job_id, worker, error="", retry_in=None, final=False):
        row = self._owned(job_id, worker)
        if row is None:
            return None
        if not final and row["attempts"] < row["max_attempts"]:
            delay = self.backoff(row["attempts"]) if retry_in is None else retry_in
            self.conn.execute("UPDATE jobs SET status = 'ready', run_at = ?, error = ?, worker = NULL, lease_until = NULL WHERE id = ?", (self.clock() + delay, error, job_id))
            return "retry"
        self.conn.execute("UPDATE jobs SET status = 'dead', finished = ?, error = ?, worker = NULL, lease_until = NULL WHERE id = ?", (self.clock(), error, job_id))
        if row["flow"]:
            self._step_failed(row["flow"], row["step"])
        return "dead"

    def recover(self):
        """Return jobs whose lease ran out, as happens when a worker crashes. Returns how many."""
        now = self.clock()
        rows = self.conn.execute("SELECT id, attempts, max_attempts, flow, step FROM jobs WHERE status = 'running' AND lease_until < ?", (now,)).fetchall()
        for r in rows:
            if r["attempts"] >= r["max_attempts"]:
                self.conn.execute("UPDATE jobs SET status = 'dead', finished = ?, error = 'lease expired', worker = NULL, lease_until = NULL WHERE id = ?", (now, r["id"]))
                if r["flow"]:
                    self._step_failed(r["flow"], r["step"])
            else:
                self.conn.execute("UPDATE jobs SET status = 'ready', run_at = ?, error = 'lease expired', worker = NULL, lease_until = NULL WHERE id = ?", (now, r["id"]))
        return len(rows)

    def get(self, job_id):
        row = self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        job = dict(row)
        job["payload"] = json.loads(job["payload"])
        job["result"] = json.loads(job["result"]) if job["result"] else None
        return job

    def stats(self):
        return {r["status"]: r["n"] for r in self.conn.execute("SELECT status, COUNT(*) n FROM jobs GROUP BY status")}

    def retry_dead(self, job_id):
        cur = self.conn.execute("UPDATE jobs SET status = 'ready', attempts = 0, run_at = ?, finished = NULL WHERE id = ? AND status = 'dead'", (self.clock(), job_id))
        return cur.rowcount == 1

    def purge(self, older_than):
        cur = self.conn.execute("DELETE FROM jobs WHERE status IN ('done', 'dead') AND finished < ?", (self.clock() - older_than,))
        return cur.rowcount

    # schedules

    def schedule(self, name, cron, task, payload=None, queue="default"):
        c = Cron(cron)
        first = c.next_after(datetime.fromtimestamp(self.clock(), timezone.utc)).timestamp()
        self.conn.execute("INSERT OR REPLACE INTO schedules (name, cron, queue, task, payload, next_run) VALUES (?, ?, ?, ?, ?, ?)", (name, cron, queue, task, json.dumps(payload), first))

    def tick(self):
        """Enqueue every schedule that is due. A long outage fires a missed schedule once, not many times."""
        now = self.clock()
        count = 0
        for s in self.conn.execute("SELECT * FROM schedules WHERE enabled = 1 AND next_run <= ?", (now,)).fetchall():
            self.enqueue(s["task"], json.loads(s["payload"]), s["queue"], unique=f"schedule:{s['name']}")
            nxt = Cron(s["cron"]).next_after(datetime.fromtimestamp(now, timezone.utc)).timestamp()
            self.conn.execute("UPDATE schedules SET next_run = ? WHERE name = ?", (nxt, s["name"]))
            count += 1
        return count

    # workflows

    def start_flow(self, name, steps, queue="default", payload=None):
        """steps: {step: {"task": name, "after": [steps], "max_attempts": n}}. Runs as a DAG."""
        for s, spec in steps.items():
            for d in spec.get("after", ()):
                if d not in steps:
                    raise QueueError(f"{s} depends on unknown step {d}")
        seen, stack = set(), set()

        def visit(s):
            if s in stack:
                raise QueueError(f"workflow has a cycle through {s}")
            if s not in seen:
                stack.add(s)
                for d in steps[s].get("after", ()):
                    visit(d)
                stack.discard(s)
                seen.add(s)
        for s in steps:
            visit(s)
        fid = uuid.uuid4().hex
        spec = {"steps": steps, "queue": queue, "payload": payload}
        self.conn.execute("INSERT INTO flows (id, name, spec, status, created) VALUES (?, ?, ?, 'running', ?)", (fid, name, json.dumps(spec), self.clock()))
        for s in steps:
            self.conn.execute("INSERT INTO steps (flow, step, status) VALUES (?, ?, 'waiting')", (fid, s))
        self._advance(fid)
        return fid

    def _spec(self, flow):
        return json.loads(self.conn.execute("SELECT spec FROM flows WHERE id = ?", (flow,)).fetchone()["spec"])

    def _advance(self, flow):
        spec = self._spec(flow)
        rows = {r["step"]: dict(r) for r in self.conn.execute("SELECT * FROM steps WHERE flow = ?", (flow,))}
        for name, s in spec["steps"].items():
            if rows[name]["status"] != "waiting":
                continue
            deps = s.get("after", ())
            if all(rows[d]["status"] == "done" for d in deps):
                inputs = {d: json.loads(rows[d]["result"]) if rows[d]["result"] else None for d in deps}
                self.conn.execute("UPDATE steps SET status = 'queued' WHERE flow = ? AND step = ?", (flow, name))
                rows[name]["status"] = "queued"
                self.enqueue(s["task"], {"input": spec["payload"], "from": inputs}, spec["queue"], max_attempts=s.get("max_attempts", 5), flow=flow, step=name)
        if all(r["status"] in ("done", "skipped", "failed") for r in rows.values()):
            status = "done" if all(r["status"] == "done" for r in rows.values()) else "failed"
            self.conn.execute("UPDATE flows SET status = ?, finished = ? WHERE id = ?", (status, self.clock(), flow))

    def _step_done(self, flow, step, result):
        self.conn.execute("UPDATE steps SET status = 'done', result = ? WHERE flow = ? AND step = ?", (json.dumps(result), flow, step))
        self._advance(flow)

    def _step_failed(self, flow, step):
        self.conn.execute("UPDATE steps SET status = 'failed' WHERE flow = ? AND step = ?", (flow, step))
        spec = self._spec(flow)["steps"]
        changed = True
        blocked = {step}
        while changed:
            changed = False
            for name, s in spec.items():
                if name not in blocked and any(d in blocked for d in s.get("after", ())):
                    blocked.add(name)
                    changed = True
                    self.conn.execute("UPDATE steps SET status = 'skipped' WHERE flow = ? AND step = ? AND status = 'waiting'", (flow, name))
        self._advance(flow)

    def flow(self, flow):
        f = self.conn.execute("SELECT * FROM flows WHERE id = ?", (flow,)).fetchone()
        if f is None:
            return None
        steps = {r["step"]: {"status": r["status"], "result": json.loads(r["result"]) if r["result"] else None} for r in self.conn.execute("SELECT * FROM steps WHERE flow = ?", (flow,))}
        return {"id": flow, "name": f["name"], "status": f["status"], "steps": steps}


class Worker:
    def __init__(self, queue, handlers, queues=("default",), name=None, lease=60):
        self.q, self.handlers, self.queues, self.lease = queue, handlers, queues, lease
        self.name = name or "worker-" + uuid.uuid4().hex[:8]
        self.stopping = threading.Event()

    def run_one(self):
        """Run at most one job and report whether there was one."""
        job = self.q.claim(self.name, self.queues, self.lease)
        if job is None:
            return False
        fn = self.handlers.get(job["task"])
        if fn is None:
            self.q.fail(job["id"], self.name, f"no handler for task {job['task']}", final=True)
            return True
        try:
            result = fn(job["payload"])
        except Retry as r:
            self.q.fail(job["id"], self.name, str(r), retry_in=r.delay)
        except Skip as s:
            self.q.fail(job["id"], self.name, str(s), final=True)
        except Exception:
            self.q.fail(job["id"], self.name, traceback.format_exc(limit=5))
        else:
            self.q.complete(job["id"], self.name, result)
        return True

    def run(self, idle=0.2):
        """Run until stopped, picking up crashed jobs and firing schedules along the way."""
        while not self.stopping.is_set():
            self.q.recover()
            self.q.tick()
            if not self.run_one():
                self.stopping.wait(idle)

    def stop(self):
        self.stopping.set()
