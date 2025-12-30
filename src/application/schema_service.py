import sqlite3
import psycopg2
from psycopg2 import sql
from src.domain.models import TableSchema


class SchemaService:

    # ==============================================
    # Generate SQL schema from SQLite
    # ==============================================
    def generate_schema(self, sqlite_path: str) -> str:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        ddl_statements = []

        for (table_name,) in tables:
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = cursor.fetchall()

            col_defs = []
            for col in columns:
                name = col[1]
                col_type = col[2].upper()

                # Normalize types
                col_type = self._map_sqlite_type_to_postgres(col_type)

                col_defs.append(f'"{name}" {col_type}')

            ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  ' + ",\n  ".join(col_defs) + "\n);"
            ddl_statements.append(ddl)

        conn.close()
        return "\n\n".join(ddl_statements)

    # ==============================================
    # Apply schema into PostgreSQL
    # ==============================================
    def apply_schema(self, postgres_url: str, schema_sql: str) -> str:
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()

        cur.execute("BEGIN;")
        cur.execute(schema_sql)
        conn.commit()

        cur.close()
        conn.close()
        return "Schema applied successfully."

    # ==============================================
    # Type Mapping
    # ==============================================
    def _map_sqlite_type_to_postgres(self, col_type: str) -> str:
        if "INT" in col_type:
            return "INTEGER"
        if "TEXT" in col_type:
            return "TEXT"
        if "REAL" in col_type or "DOUBLE" in col_type or "FLOAT" in col_type:
            return "DOUBLE PRECISION"
        if "NUMERIC" in col_type or "DECIMAL" in col_type:
            return "NUMERIC"
        if "DATE" in col_type:
            return "DATE"
        if "DATETIME" in col_type:
            return "TIMESTAMP"

        return "TEXT"
