import sqlite3
from typing import List, Dict, Generator, Any, Tuple


class SQLiteReader:
    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path

    # ---------------------------
    # Connect
    # ---------------------------
    def connect(self):
        return sqlite3.connect(self.sqlite_path)

    # ---------------------------
    # List tables
    # ---------------------------
    def get_tables(self) -> List[str]:
        conn = self.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    # ---------------------------
    # Read table schema
    # ---------------------------
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        conn = self.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f'PRAGMA table_info("{table}")')
            cols = cursor.fetchall()

            return [
                {
                    "cid": col[0],
                    "name": col[1],
                    "type": col[2],
                    "notnull": bool(col[3]),
                    "default": col[4],
                    "pk": bool(col[5]),
                }
                for col in cols
            ]
        finally:
            conn.close()

    # ---------------------------
    # Read rows in batches (safe generator)
    # ---------------------------
    def read_rows(
        self,
        table: str,
        batch_size: int = 1000
    ) -> Generator[List[Tuple], None, None]:

        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(f'SELECT * FROM "{table}"')

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows
        finally:
            conn.close()
