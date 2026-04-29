from __future__ import annotations

import platform

from rq import Connection, Worker

from app.services.sync_queue import SYNC_QUEUE_NAME, get_redis


def main() -> None:
    connection = get_redis()
    with Connection(connection):
        # macOS can crash forked work-horses when Objective-C runtime is initialized
        # (common with DB/GSS/Kerberos libs). Use SimpleWorker locally on Darwin.
        worker_class = Worker
        if platform.system() == "Darwin":
            from rq import SimpleWorker

            worker_class = SimpleWorker
        worker = worker_class([SYNC_QUEUE_NAME])
        worker.work(with_scheduler=False)


if __name__ == "__main__":
    main()
