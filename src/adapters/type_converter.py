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

    # default fallback
    DEFAULT_TYPE = "TEXT"

    def map_sqlite_to_pg(self, sqlite_type: str) -> str:
        sqlite_type = sqlite_type.upper()

        for key in self.SQLITE_TO_PG:
            if key in sqlite_type:
                return self.SQLITE_TO_PG[key]

        return self.DEFAULT_TYPE
