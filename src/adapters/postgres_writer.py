from typing import List, Dict
import psycopg2
from psycopg2.extras import execute_batch


class PostgreSQLWriter:

    def __init__(self, postgres_url: str):
        self.postgres_url = postgres_url

    # ---------------------------
    # Connect
    # ---------------------------
    def connect(self):
        return psycopg2.connect(self.postgres_url)

    # ---------------------------
    # Create table dynamically
    # ---------------------------
    def create_table(self, table_name: str, schema: List[Dict], converter):
        conn = self.connect()
        cursor = conn.cursor()

        # Convert SQLite column types
        columns_sql = []
        for col in schema:
            pg_type = converter.map_sqlite_to_pg(col["type"])
            columns_sql.append(f"\"{col['name']}\" {pg_type}")

        # PostgreSQL primary key handling
        pk_cols = [col["name"] for col in schema if col["pk"] == 1]
        pk_sql = f", PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {", ".join(columns_sql)}
                {pk_sql}
            );
        """

        cursor.execute(create_sql)
        conn.commit()
        conn.close()

    # ---------------------------
    # Insert rows into table
    # ---------------------------
    def insert_rows(self, table: str, schema: List[Dict], rows: List[tuple]):
        conn = self.connect()
        cursor = conn.cursor()

        column_names = [col["name"] for col in schema]
        col_placeholders = ", ".join(["%s"] * len(column_names))

        sql = f"""
            INSERT INTO "{table}" ({", ".join(column_names)})
            VALUES ({col_placeholders})
        """

        execute_batch(cursor, sql, rows, page_size=1000)

        conn.commit()
        conn.close()
