class TypeConverter:
    SQLITE_TO_PG = {
        "INTEGER": "BIGINT",
        "INT": "BIGINT",
        "TEXT": "TEXT",
        "NUMERIC": "NUMERIC",
        "REAL": "DOUBLE PRECISION",
        "DOUBLE": "DOUBLE PRECISION",
        "BOOLEAN": "BOOLEAN",
        "DATETIME": "TIMESTAMP",
        "DATE": "DATE",
    }

    DEFAULT_TYPE = "TEXT"

    @staticmethod
    def sqlite_to_postgres(sqlite_type: str) -> str:
        if not sqlite_type:
            return TypeConverter.DEFAULT_TYPE

        sqlite_type = sqlite_type.upper()

        for key, pg_type in TypeConverter.SQLITE_TO_PG.items():
            if key in sqlite_type:
                return pg_type

        return TypeConverter.DEFAULT_TYPE
