from typing import List, Dict
from src.application.schema.type_converter import TypeConverter


class SchemaValidationResult:
    def __init__(self, valid: bool, errors: List[str]):
        self.valid = valid
        self.errors = errors


class SchemaValidator:
    """
    Validates compatibility between SQLite schema and PostgreSQL schema.
    """

    def __init__(self):
        self.converter = TypeConverter()

    def validate(
        self,
        sqlite_schema: List[Dict],
        postgres_schema: List[Dict],
        table_name: str,
    ) -> SchemaValidationResult:

        errors: List[str] = []

        sqlite_cols = {c["name"]: c for c in sqlite_schema}
        pg_cols = {c["name"]: c for c in postgres_schema}

        # 1️⃣ Column existence
        for col in sqlite_cols:
            if col not in pg_cols:
                errors.append(
                    f"Table '{table_name}': column '{col}' missing in PostgreSQL"
                )

        # 2️⃣ Extra columns in destination
        for col in pg_cols:
            if col not in sqlite_cols:
                errors.append(
                    f"Table '{table_name}': extra column '{col}' in PostgreSQL"
                )

        # 3️⃣ Type compatibility
        for col_name, sqlite_col in sqlite_cols.items():
            if col_name not in pg_cols:
                continue

            sqlite_type = sqlite_col["type"]
            expected_pg_type = self.converter.sqlite_to_postgres(sqlite_type)
            actual_pg_type = pg_cols[col_name]["type"].upper()

            if expected_pg_type not in actual_pg_type:
                errors.append(
                    f"Table '{table_name}', column '{col_name}': "
                    f"type mismatch (expected {expected_pg_type}, got {actual_pg_type})"
                )

        # 4️⃣ Primary key comparison
        sqlite_pk = {
            c["name"] for c in sqlite_schema if c.get("pk")
        }
        pg_pk = {
            c["name"] for c in postgres_schema if c.get("pk")
        }

        if sqlite_pk != pg_pk:
            errors.append(
                f"Table '{table_name}': primary key mismatch "
                f"(sqlite={sqlite_pk}, postgres={pg_pk})"
            )

        return SchemaValidationResult(
            valid=len(errors) == 0,
            errors=errors,
        )
