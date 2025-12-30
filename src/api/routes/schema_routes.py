from fastapi import APIRouter, HTTPException
from src.domain.models import (
    GenerateSchemaRequest,
    ApplySchemaRequest
)
from src.application.schema_service import SchemaService

router = APIRouter(prefix="/schema", tags=["Schema"])

schema_service = SchemaService()

@router.post("/generate")
def generate_schema(req: GenerateSchemaRequest):
    try:
        sql_schema = schema_service.generate_schema(req.sqlite_path)
        return {"schema_sql": sql_schema}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/apply")
def apply_schema(req: ApplySchemaRequest):
    try:
        result = schema_service.apply_schema(req.postgres_url, req.schema_sql)
        return {"status": "success", "message": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
