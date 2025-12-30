import uuid
import threading

from src.domain.migrator import Migrator
from src.domain.models import (
    StartMigrationRequest,
    StartMigrationResponse,
    MigrationJob,
    JobStatus,
    JobStatusResponse,
    JobErrorResponse
)
from src.infrastructure.job_manager import JobManager


class MigrationService:

    def __init__(self):
        self.job_manager = JobManager()

    # ==============================================
    # Start Migration Job (REAL MIGRATOR)
    # ==============================================
    def start_migration(self, req: StartMigrationRequest) -> StartMigrationResponse:
        job_id = str(uuid.uuid4())

        job = MigrationJob(job_id=job_id, status=JobStatus.PENDING)
        self.job_manager.create_job(job)

        # Create migrator instance
        migrator = Migrator(
            sqlite_path=req.sqlite_path,
            postgres_url=req.postgres_url,
            batch_size=req.batch_size or 1000
        )

        # Run migration in background thread
        t = threading.Thread(target=migrator.run, args=(job_id,), daemon=True)
        t.start()

        job.status = JobStatus.RUNNING
        self.job_manager.update_job(job)

        return StartMigrationResponse(
            job_id=job_id,
            status=job.status,
            message="Migration job started successfully."
        )

    # ==============================================
    # Query Job Status
    # ==============================================
    def get_job_status(self, job_id: str) -> JobStatusResponse:
        job = self.job_manager.get_job(job_id)

        return JobStatusResponse(
            job_id=job.job_id,
            status=job.status,
            progress=job.progress,
            processed_rows=job.processed_rows,
            total_rows=job.total_rows,
            error=job.errors[-1] if job.errors else None
        )

    # ==============================================
    # Get Errors
    # ==============================================
    def get_job_errors(self, job_id: str) -> JobErrorResponse:
        job = self.job_manager.get_job(job_id)
        return JobErrorResponse(job_id=job.job_id, errors=job.errors)

    # ==============================================
    # Cancel Job
    # ==============================================
    def cancel_job(self, job_id: str):
        job = self.job_manager.get_job(job_id)
        job.status = JobStatus.CANCELLED
        self.job_manager.update_job(job)
