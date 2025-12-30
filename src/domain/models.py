from enum import Enum
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, List


# ==============================================
# JOB STATUS
# ==============================================
class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# ==============================================
# REQUEST MODELS
# ==============================================
class GenerateSchemaRequest(BaseModel):
    sqlite_path: str


class ApplySchemaRequest(BaseModel):
    postgres_url: str
    schema_sql: str


class StartMigrationRequest(BaseModel):
    sqlite_path: str
    postgres_url: str
    batch_size: int = 500


# ==============================================
# RESPONSE MODELS
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


# ==============================================
# INTERNAL DOMAIN MODELS
# ==============================================
class TableSchema(BaseModel):
    name: str
    columns: Dict[str, str]  # nombre_columna: tipo_sql


class MigrationConfig(BaseModel):
    sqlite_path: str
    postgres_url: str
    batch_size: int = 500


# ==============================================
# INTERNAL JOB MANAGEMENT MODEL
# ==============================================
class MigrationJob(BaseModel):
    job_id: str
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = datetime.utcnow()
    updated_at: datetime = datetime.utcnow()
    progress: float = 0.0
    processed_rows: int = 0
    total_rows: Optional[int] = None
    errors: List[str] = []
