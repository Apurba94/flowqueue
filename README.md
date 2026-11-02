# FlowQueue

A durable job queue and workflow engine in pure Python. All state lives in one SQLite file, so jobs survive restarts and you need no separate server.

```python
from flowqueue import Queue, Worker

q = Queue("jobs.db")
q.enqueue("send_report", {"week": 44}, max_attempts=5)
q.schedule("nightly-backup", "0 2 * * *", "backup")

def send_report(payload):
    print("sending week", payload["week"])

Worker(q, {"send_report": send_report, "backup": lambda p: None}).run()
```

## Features

- **Retries:** exponential backoff with jitter, then a dead-letter state you can inspect and retry
- **Crash recovery:** a worker holds a job under a lease, and if it dies the job returns to the queue. A worker that lost its lease cannot overwrite a newer result
- **Priorities, delays and separate queues**
- **Unique jobs:** a duplicate of a live job returns the existing id
- **Cron schedules:** standard five-field expressions, including ranges, steps, names and the rule that a limited day and weekday mean either one. A missed schedule fires once after an outage, not hundreds of times
- **Workflows:** steps form a DAG, results pass to the steps after them, and a failure skips only what depends on it

```python
q.start_flow("weekly", {
    "fetch":  {"task": "fetch"},
    "clean":  {"task": "clean", "after": ["fetch"]},
    "stats":  {"task": "stats", "after": ["fetch"]},
    "report": {"task": "report", "after": ["clean", "stats"]},
})
```

## How it is tested

The clock is a parameter, so tests move time forward instead of waiting. Six threads race over 200 jobs, and each must run exactly once. The fast cron calculation is checked against a plain minute-by-minute search.

## Limits

Delivery is at-least-once: after a crash a job may run twice, so handlers should be safe to repeat. Long jobs should call `heartbeat`, which `Worker` does not do for you. SQLite allows one writer at a time, so expect hundreds of jobs per second, not many thousands, and keep the file on a local disk rather than a network share. Schedules run in UTC.

## Tests

```
python -m unittest discover -s tests
```

MIT licence.
