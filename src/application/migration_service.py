import uuid
import threading

from src.application.dtos import (
    StartMigrationRequest,
    StartMigrationResponse,
    JobStatusResponse,
    JobErrorResponse,
)
from src.application.jobs.job_manager import JobManager
from src.application.migration.migration_runner import MigrationRunner
from src.domain.models import MigrationJob


class MigrationService:
    def __init__(self):
        self.job_manager = JobManager()

    def start_migration(self, req: StartMigrationRequest) -> StartMigrationResponse:
        job_id = str(uuid.uuid4())
        job = MigrationJob(job_id=job_id)

        self.job_manager.create(job)

        thread = threading.Thread(
            target=self._run_async,
            args=(job_id, req),
            daemon=True,
        )
        thread.start()

        job.mark_running()
        self.job_manager.update(job)

        return StartMigrationResponse(
            job_id=job_id,
            status=job.status,
            message="Migration job started successfully",
        )

    def _run_async(self, job_id: str, req: StartMigrationRequest):
        runner = MigrationRunner(self.job_manager)
        runner.run(job_id, req)

    def get_job_status(self, job_id: str) -> JobStatusResponse:
        job = self.job_manager.get(job_id)
        return JobStatusResponse(
            job_id=job.job_id,
            status=job.status,
            progress=job.progress,
            processed_rows=job.processed_rows,
            total_rows=job.total_rows,
            error=job.errors[-1] if job.errors else None,
        )

    def get_job_errors(self, job_id: str) -> JobErrorResponse:
        job = self.job_manager.get(job_id)
        return JobErrorResponse(job_id=job.job_id, errors=job.errors)

    def cancel_job(self, job_id: str):
        job = self.job_manager.get(job_id)
        job.cancel()
        self.job_manager.update(job)
