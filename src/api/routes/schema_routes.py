from fastapi import APIRouter, HTTPException, status

from src.application.schema_service import SchemaService
from src.application.dtos import (
    GenerateSchemaRequest,
    ApplySchemaRequest,
)

router = APIRouter(prefix="/schema", tags=["Schema"])

schema_service = SchemaService()


@router.post(
    "/generate",
    status_code=status.HTTP_200_OK,
)
def generate_schema(req: GenerateSchemaRequest):
    try:
        sql_schema = schema_service.generate_schema(req.sqlite_path)
        return {"schema_sql": sql_schema}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate schema",
        )


@router.post(
    "/apply",
    status_code=status.HTTP_200_OK,
)
def apply_schema(req: ApplySchemaRequest):
    try:
        schema_service.apply_schema(req.postgres_url, req.schema_sql)
        return {
            "status": "success",
            "message": "Schema applied successfully",
        }

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to apply schema",
        )
