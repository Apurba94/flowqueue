import os
import shutil
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone

from flowqueue import Cron, Queue, QueueError, Retry, Skip, Worker


class Clock:
    def __init__(self):
        self.t = 1_700_000_000.0

    def __call__(self):
        return self.t


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "q.db")
        self.clock = Clock()
        self.q = Queue(self.path, clock=self.clock, seed=1)

    def tearDown(self):
        self.q.close()
        shutil.rmtree(self.dir, ignore_errors=True)


class TestQueue(Base):
    def test_claim_complete(self):
        j = self.q.enqueue("t", {"a": 1})
        job = self.q.claim("w")
        self.assertEqual((job["id"], job["payload"], job["attempts"]), (j, {"a": 1}, 1))
        self.assertIsNone(self.q.claim("w2"))
        self.assertTrue(self.q.complete(j, "w", 42))
        self.assertEqual(self.q.get(j)["result"], 42)
        self.assertEqual(self.q.stats(), {"done": 1})

    def test_priority_then_time(self):
        a = self.q.enqueue("t")
        self.clock.t += 1
        b = self.q.enqueue("t", priority=5)
        self.clock.t += 1
        c = self.q.enqueue("t")
        self.assertEqual([self.q.claim("w")["id"] for _ in range(3)], [b, a, c])

    def test_delay(self):
        self.q.enqueue("t", delay=10)
        self.assertIsNone(self.q.claim("w"))
        self.clock.t += 10
        self.assertIsNotNone(self.q.claim("w"))

    def test_queues_are_separate(self):
        self.q.enqueue("t", queue="mail")
        self.assertIsNone(self.q.claim("w", ("default",)))
        self.assertIsNotNone(self.q.claim("w", ("default", "mail")))

    def test_retry_backoff_then_dead(self):
        j = self.q.enqueue("t", max_attempts=3)
        delays = []
        for i in range(2):
            self.q.claim("w")
            self.assertEqual(self.q.fail(j, "w", "boom"), "retry")
            delay = self.q.get(j)["run_at"] - self.clock.t
            delays.append(delay)
            self.assertIsNone(self.q.claim("w"))
            self.clock.t += delay
        self.assertLess(delays[0], 2 + 1e-9)
        self.assertGreater(delays[1], 1.99)
        self.q.claim("w")
        self.assertEqual(self.q.fail(j, "w", "boom"), "dead")
        self.assertEqual(self.q.get(j)["status"], "dead")
        self.assertTrue(self.q.retry_dead(j))
        self.assertEqual(self.q.claim("w")["attempts"], 1)

    def test_backoff_is_capped(self):
        self.assertLessEqual(self.q.backoff(100), 3600)

    def test_unique_jobs(self):
        a = self.q.enqueue("t", unique="k")
        self.assertEqual(self.q.enqueue("t", unique="k"), a)
        self.q.claim("w")
        self.assertEqual(self.q.enqueue("t", unique="k"), a)
        self.q.complete(a, "w")
        self.assertNotEqual(self.q.enqueue("t", unique="k"), a)

    def test_purge(self):
        j = self.q.enqueue("t")
        self.q.claim("w")
        self.q.complete(j, "w")
        self.assertEqual(self.q.purge(100), 0)
        self.clock.t += 101
        self.assertEqual(self.q.purge(100), 1)


class TestCrashRecovery(Base):
    def test_lease_expiry_returns_the_job(self):
        j = self.q.enqueue("t")
        self.q.claim("dead-worker", lease=30)
        self.clock.t += 29
        self.assertEqual(self.q.recover(), 0)
        self.clock.t += 2
        self.assertEqual(self.q.recover(), 1)
        self.assertEqual(self.q.claim("w2")["id"], j)

    def test_heartbeat_extends_the_lease(self):
        j = self.q.enqueue("t")
        self.q.claim("w", lease=30)
        self.clock.t += 20
        self.assertTrue(self.q.heartbeat(j, "w", 30))
        self.clock.t += 20
        self.assertEqual(self.q.recover(), 0)

    def test_late_finish_from_a_lost_worker_is_ignored(self):
        j = self.q.enqueue("t")
        self.q.claim("slow", lease=10)
        self.clock.t += 11
        self.q.recover()
        self.q.claim("fast")
        self.assertFalse(self.q.complete(j, "slow", "stale"))
        self.assertTrue(self.q.complete(j, "fast", "fresh"))
        self.assertEqual(self.q.get(j)["result"], "fresh")

    def test_poison_job_stops_after_max_attempts(self):
        j = self.q.enqueue("t", max_attempts=2)
        for _ in range(2):
            self.q.claim("w", lease=1)
            self.clock.t += 2
            self.q.recover()
        self.assertEqual(self.q.get(j)["status"], "dead")

    def test_data_survives_a_new_connection(self):
        j = self.q.enqueue("t")
        self.q.close()
        again = Queue(self.path, clock=self.clock)
        self.assertEqual(again.claim("w")["id"], j)
        again.close()


class TestConcurrency(Base):
    def test_each_job_runs_exactly_once_across_threads(self):
        q = Queue(self.path)
        for i in range(200):
            q.enqueue("t", i)
        done, lock = [], threading.Lock()

        def handler(i):
            with lock:
                done.append(i)

        def run(n):
            w = Worker(Queue(self.path), {"t": handler}, name=f"w{n}")
            while w.run_one():
                pass
            w.q.close()
        threads = [threading.Thread(target=run, args=(n,)) for n in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(done), list(range(200)))
        self.assertEqual(q.stats(), {"done": 200})
        q.close()


class TestWorker(Base):
    def test_handlers(self):
        calls = []

        def ok(p):
            calls.append(p)
            return p * 2

        def flaky(p):
            calls.append("flaky")
            if len([c for c in calls if c == "flaky"]) < 2:
                raise ValueError("first time fails")
            return "ok"

        def later(p):
            raise Retry(100)

        def never(p):
            raise Skip("not worth retrying")

        w = Worker(self.q, {"ok": ok, "flaky": flaky, "later": later, "never": never}, name="w")
        a = self.q.enqueue("ok", 4)
        w.run_one()
        self.assertEqual(self.q.get(a)["result"], 8)
        b = self.q.enqueue("flaky")
        w.run_one()
        self.assertEqual(self.q.get(b)["status"], "ready")
        self.assertIn("first time fails", self.q.get(b)["error"])
        self.clock.t += 100
        w.run_one()
        self.assertEqual(self.q.get(b)["result"], "ok")
        c = self.q.enqueue("later")
        w.run_one()
        self.assertAlmostEqual(self.q.get(c)["run_at"], self.clock.t + 100)
        d = self.q.enqueue("never")
        w.run_one()
        self.assertEqual(self.q.get(d)["status"], "dead")
        e = self.q.enqueue("unknown")
        w.run_one()
        self.assertEqual(self.q.get(e)["status"], "dead")


class TestCron(unittest.TestCase):
    def test_parse(self):
        c = Cron("*/15 9-17 * * mon-fri")
        self.assertEqual(c.sets[0], {0, 15, 30, 45})
        self.assertEqual(c.sets[4], {1, 2, 3, 4, 5})
        self.assertEqual(Cron("0 0 * * 7").sets[4], {0})
        self.assertEqual(Cron("5/20 * * * *").sets[0], {5, 25, 45})

    def test_bad(self):
        for bad in ("* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "a * * * *", "*/0 * * * *", "5-1 * * * *"):
            with self.assertRaises(QueueError):
                Cron(bad)

    def test_day_and_weekday_mean_either(self):
        c = Cron("0 0 13 * fri")
        self.assertTrue(c.matches(datetime(2026, 11, 13)))     # a Friday and the 13th
        self.assertTrue(c.matches(datetime(2026, 11, 6)))      # a Friday
        self.assertTrue(c.matches(datetime(2026, 10, 13)))     # the 13th
        self.assertFalse(c.matches(datetime(2026, 11, 7)))

    def test_leap_day(self):
        self.assertEqual(Cron("0 0 29 2 *").next_after(datetime(2026, 3, 1)), datetime(2028, 2, 29))

    def test_impossible_date_is_reported(self):
        with self.assertRaises(QueueError):
            Cron("0 0 31 2 *").next_after(datetime(2026, 1, 1))

    def test_next_after_matches_minute_by_minute_search(self):
        import random
        rng = random.Random(2)
        exprs = ["*/7 * * * *", "0 */6 * * *", "30 2 * * 1", "0 0 1 * *", "15 10 * 3 *", "0 12 15 * mon", "59 23 31 12 *", "*/20 8-10 * * 1-5"]
        for _ in range(60):
            e = rng.choice(exprs)
            c = Cron(e)
            t = datetime(2026, 1, 1) + timedelta(minutes=rng.randint(0, 500000))
            s = t + timedelta(minutes=1)
            for _ in range(60 * 24 * 370):
                if c.matches(s):
                    break
                s += timedelta(minutes=1)
            self.assertEqual(c.next_after(t), s, e)


class TestSchedules(Base):
    def test_fires_when_due(self):
        self.q.schedule("nightly", "0 3 * * *", "backup")
        self.assertEqual(self.q.tick(), 0)
        self.clock.t += 86400
        self.assertEqual(self.q.tick(), 1)
        self.assertEqual(self.q.claim("w")["task"], "backup")
        self.assertEqual(self.q.tick(), 0)

    def test_outage_fires_once(self):
        self.q.schedule("minute", "* * * * *", "x")
        self.clock.t += 3600
        self.assertEqual(self.q.tick(), 1)
        self.assertEqual(self.q.tick(), 0)
        self.assertEqual(self.q.stats(), {"ready": 1})


class TestFlows(Base):
    STEPS = {"fetch": {"task": "fetch"}, "clean": {"task": "clean", "after": ["fetch"]}, "stats": {"task": "stats", "after": ["fetch"]}, "report": {"task": "report", "after": ["clean", "stats"]}}

    def worker(self, fail=None):
        def make(name):
            def h(p):
                if name == fail:
                    raise Skip("broken")
                return {"name": name, "got": sorted(p["from"])}
            return h
        return Worker(self.q, {k: make(k) for k in self.STEPS}, name="w")

    def test_diamond_runs_in_order(self):
        f = self.q.start_flow("etl", self.STEPS, payload={"day": 1})
        w = self.worker()
        order = []
        for _ in range(4):
            job = self.q.conn.execute("SELECT step FROM jobs WHERE status = 'ready'").fetchall()
            order.append(sorted(r["step"] for r in job))
            w.run_one()
        self.assertEqual(order[0], ["fetch"])
        self.assertEqual(order[1], ["clean", "stats"])
        result = self.q.flow(f)
        self.assertEqual(result["status"], "done")
        self.assertEqual(result["steps"]["report"]["result"]["got"], ["clean", "stats"])

    def test_failure_skips_what_depends_on_it(self):
        f = self.q.start_flow("etl", self.STEPS)
        w = self.worker(fail="clean")
        while w.run_one():
            pass
        result = self.q.flow(f)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["steps"]["clean"]["status"], "failed")
        self.assertEqual(result["steps"]["report"]["status"], "skipped")
        self.assertEqual(result["steps"]["stats"]["status"], "done")

    def test_cycles_and_bad_steps_are_refused(self):
        with self.assertRaises(QueueError):
            self.q.start_flow("x", {"a": {"task": "t", "after": ["b"]}, "b": {"task": "t", "after": ["a"]}})
        with self.assertRaises(QueueError):
            self.q.start_flow("x", {"a": {"task": "t", "after": ["zz"]}})


if __name__ == "__main__":
    unittest.main()
