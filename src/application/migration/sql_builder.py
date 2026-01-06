class SQLBuilder:
    @staticmethod
    def build_create_table(table: str, schema, converter) -> str:
        cols = []
        pk = []

        for col in schema:
            pg_type = converter.sqlite_to_postgres(col["type"])
            cols.append(f'"{col["name"]}" {pg_type}')
            if col["pk"]:
                pk.append(f'"{col["name"]}"')

        pk_sql = f", PRIMARY KEY ({', '.join(pk)})" if pk else ""

        return f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            {", ".join(cols)}
            {pk_sql}
        );
        """
