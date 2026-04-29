from __future__ import annotations

from app.models.contracts import JobStatus


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobStatus] = {}

    def upsert(self, job: JobStatus) -> JobStatus:
        self._jobs[str(job.event_id)] = job
        return job

    def get(self, event_id: str) -> JobStatus | None:
        return self._jobs.get(event_id)
