from fastapi import APIRouter, HTTPException, status

from src.application.migration_service import MigrationService
from src.application.dtos import (
    StartMigrationRequest,
    StartMigrationResponse,
    JobStatusResponse,
    JobErrorResponse,
)

router = APIRouter(prefix="/migration", tags=["Migration"])

migration_service = MigrationService()


@router.post(
    "/start",
    response_model=StartMigrationResponse,
    status_code=status.HTTP_201_CREATED,
)
def start_migration(req: StartMigrationRequest):
    try:
        return migration_service.start_migration(req)

    except ValueError as e:
        # errores de validación / negocio
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    except Exception:
        # error inesperado
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start migration job",
        )


@router.get(
    "/status/{job_id}",
    response_model=JobStatusResponse,
)
def get_status(job_id: str):
    try:
        return migration_service.get_job_status(job_id)

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve job status",
        )


@router.get(
    "/errors/{job_id}",
    response_model=JobErrorResponse,
)
def get_errors(job_id: str):
    try:
        return migration_service.get_job_errors(job_id)

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve job errors",
        )


@router.delete(
    "/cancel/{job_id}",
    status_code=status.HTTP_200_OK,
)
def cancel_job(job_id: str):
    try:
        migration_service.cancel_job(job_id)
        return {"status": "cancelled"}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel job",
        )
