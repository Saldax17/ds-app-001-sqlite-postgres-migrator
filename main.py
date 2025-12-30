from fastapi import FastAPI
from src.api.routes.schema_routes import router as schema_router
from src.api.routes.migration_routes import router as migration_router

app = FastAPI(
    title="SQLite → PostgreSQL Migrator",
    version="1.0.0",
    description="Servicio universal de migración entre bases de datos."
)

app.include_router(schema_router)
app.include_router(migration_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

