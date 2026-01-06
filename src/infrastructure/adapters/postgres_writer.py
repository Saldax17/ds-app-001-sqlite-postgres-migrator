from typing import List, Dict, Tuple
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
    # Execute DDL / generic SQL
    # ---------------------------
    def execute_sql(self, sql: str) -> None:
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------------------------
    # Insert rows (batch)
    # ---------------------------
    def insert_rows(
        self,
        table: str,
        columns: List[str],
        rows: List[Tuple],
    ) -> None:
        if not rows:
            return

        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                col_names = ", ".join(f'"{c}"' for c in columns)
                placeholders = ", ".join(["%s"] * len(columns))

                sql = f"""
                    INSERT INTO "{table}" ({col_names})
                    VALUES ({placeholders})
                """

                execute_batch(cursor, sql, rows, page_size=1000)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------------------------
    # Read table schema (for validation)
    # ---------------------------
    def get_table_schema(self, table: str) -> List[Dict]:
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        a.attname AS column_name,
                        t.typname AS data_type,
                        COALESCE(i.indisprimary, false) AS is_primary
                    FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_type t ON a.atttypid = t.oid
                    LEFT JOIN pg_index i
                        ON c.oid = i.indrelid
                        AND a.attnum = ANY(i.indkey)
                    WHERE c.relname = %s
                      AND a.attnum > 0
                      AND NOT a.attisdropped;
                    """,
                    (table,),
                )

                rows = cursor.fetchall()

                return [
                    {
                        "name": row[0],
                        "type": row[1].upper(),
                        "pk": bool(row[2]),
                    }
                    for row in rows
                ]
        finally:
            conn.close()
