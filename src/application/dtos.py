from typing import Optional, List
from pydantic import BaseModel, Field
from src.domain.models import JobStatus


# ==============================================
# REQUEST DTOs
# ==============================================

class StartMigrationRequest(BaseModel):
    sqlite_path: str = Field(..., description="Path to the source SQLite database")
    postgres_url: str = Field(..., description="PostgreSQL connection URL")
    batch_size: Optional[int] = Field(
        default=1000,
        gt=0,
        description="Number of rows to process per batch"
    )


# ==============================================
# RESPONSE DTOs
# ==============================================

class StartMigrationResponse(BaseModel):
    job_id: str
    status: JobStatus
    message: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    progress: Optional[float] = None
    processed_rows: Optional[int] = None
    total_rows: Optional[int] = None
    error: Optional[str] = None


class JobErrorResponse(BaseModel):
    job_id: str
    errors: List[str]
