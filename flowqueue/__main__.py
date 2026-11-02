"""Inspect a queue: python -m flowqueue stats|recover|purge DAYS FILE, or cron "EXPR" to preview."""
import sys
from datetime import datetime, timezone

from . import Cron, Queue


def main(argv):
    if len(argv) == 3 and argv[1] == "cron":
        t = datetime.now(timezone.utc)
        c = Cron(argv[2])
        for _ in range(5):
            t = c.next_after(t)
            print(t.strftime("%Y-%m-%d %H:%M UTC"))
        return 0
    if len(argv) == 3 and argv[1] == "stats":
        for k, v in sorted(Queue(argv[2]).stats().items()):
            print(f"{k:<10}{v}")
        return 0
    if len(argv) == 3 and argv[1] == "recover":
        print(Queue(argv[2]).recover(), "jobs returned to the queue")
        return 0
    if len(argv) == 4 and argv[1] == "purge":
        print(Queue(argv[3]).purge(float(argv[2]) * 86400), "old jobs removed")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
