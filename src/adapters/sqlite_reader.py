import sqlite3
from typing import List, Dict, Generator, Any


class SQLiteReader:

    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path

    # ---------------------------
    # Connect
    # ---------------------------
    def connect(self):
        return sqlite3.connect(self.sqlite_path)

    # ---------------------------
    # List tables in the DB
    # ---------------------------
    def get_tables(self) -> List[str]:
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]

        conn.close()
        return tables

    # ---------------------------
    # Read table schema
    # ---------------------------
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()

        conn.close()

        return [
            {
                "cid": col[0],
                "name": col[1],
                "type": col[2],
                "notnull": col[3],
                "default": col[4],
                "pk": col[5]
            }
            for col in cols
        ]

    # ---------------------------
    # Stream rows in batches
    # ---------------------------
    def read_rows(self, table: str, batch_size: int) -> Generator[List[tuple], None, None]:
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table}")

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield rows

        conn.close()
