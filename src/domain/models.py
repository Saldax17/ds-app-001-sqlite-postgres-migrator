from enum import Enum
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class MigrationJob(BaseModel):
    job_id: str
    status: JobStatus = JobStatus.PENDING

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    progress: float = 0.0
    processed_rows: int = 0
    total_rows: Optional[int] = None

    errors: List[str] = Field(default_factory=list)

    # -------- Domain behavior (reglas) --------
    def mark_running(self):
        self.status = JobStatus.RUNNING
        self.updated_at = datetime.utcnow()

    def mark_completed(self):
        self.status = JobStatus.COMPLETED
        self.progress = 100.0
        self.updated_at = datetime.utcnow()

    def mark_failed(self, error: str):
        self.status = JobStatus.FAILED
        self.errors.append(error)
        self.updated_at = datetime.utcnow()

    def cancel(self):
        self.status = JobStatus.CANCELLED
        self.updated_at = datetime.utcnow()
