from fastapi import APIRouter, HTTPException
from src.domain.models import (
    StartMigrationRequest,
    StartMigrationResponse,
    JobStatusResponse,
    JobErrorResponse
)
from src.application.migration_service import MigrationService

router = APIRouter(prefix="/migration", tags=["Migration"])

migration_service = MigrationService()

@router.post("/start", response_model=StartMigrationResponse)
def start_migration(req: StartMigrationRequest):
    try:
        job = migration_service.start_migration(req)
        return job
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/status/{job_id}", response_model=JobStatusResponse)
def get_status(job_id: str):
    try:
        status = migration_service.get_job_status(job_id)
        return status
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/errors/{job_id}", response_model=JobErrorResponse)
def get_errors(job_id: str):
    try:
        errors = migration_service.get_job_errors(job_id)
        return errors
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/cancel/{job_id}")
def cancel_job(job_id: str):
    try:
        migration_service.cancel_job(job_id)
        return {"status": "cancelled"}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
