from src.domain.models import MigrationJob
from typing import Dict


class JobManager:

    def __init__(self):
        self.jobs: Dict[str, MigrationJob] = {}

    def create_job(self, job: MigrationJob):
        self.jobs[job.job_id] = job

    def get_job(self, job_id: str) -> MigrationJob:
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
        return self.jobs[job_id]

    def update_job(self, job: MigrationJob):
        self.jobs[job.job_id] = job
