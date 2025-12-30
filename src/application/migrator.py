import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from src.domain.models import MigrationConfig, MigrationJob, JobStatus
from src.infrastructure.job_manager import JobManager


class Migrator:

    def __init__(self, job_manager: JobManager):
        self.job_manager = job_manager

    # ==============================================
    # COUNT ROWS IN SQLITE TABLE
    # ==============================================
    def _count_rows(self, sqlite_path: str, table: str) -> int:
        conn = sqlite3.connect(sqlite_path)
        cur = conn.cursor()

        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total = cur.fetchone()[0]

        conn.close()
        return total

    # ==============================================
    # FETCH BATCH OF ROWS
    # ==============================================
    def _fetch_batch(self, sqlite_path: str, table: str, offset: int, limit: int):
        conn = sqlite3.connect(sqlite_path)
        cur = conn.cursor()

        cur.execute(f"SELECT * FROM {table} LIMIT {limit} OFFSET {offset}")
        rows = cur.fetchall()

        col_names = [desc[0] for desc in cur.description]

        conn.close()
        return col_names, rows

    # ==============================================
    # INSERT ROWS INTO POSTGRES
    # ==============================================
    def _insert_batch(self, postgres_url: str, table: str, columns: list, rows: list):
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()

        col_list = ", ".join([f'"{c}"' for c in columns])
        insert_query = f'INSERT INTO "{table}" ({col_list}) VALUES %s'

        execute_values(cur, insert_query, rows)
        conn.commit()

        cur.close()
        conn.close()

    # ==============================================
    # MAIN MIGRATOR LOGIC
    # ==============================================
    def run(self, job_id: str, config: MigrationConfig):
        job = self.job_manager.get_job(job_id)

        job.status = JobStatus.RUNNING
        self.job_manager.update_job(job)

        try:
            conn = sqlite3.connect(config.sqlite_path)
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [t[0] for t in cur.fetchall()]
            conn.close()

            for table in tables:

                total_rows = self._count_rows(config.sqlite_path, table)
                job.total_rows = total_rows
                self.job_manager.update_job(job)

                offset = 0

                while offset < total_rows:
                    columns, rows = self._fetch_batch(
                        config.sqlite_path,
                        table,
                        offset,
                        config.batch_size
                    )

                    if rows:
                        self._insert_batch(
                            config.postgres_url,
                            table,
                            columns,
                            rows
                        )

                        job.processed_rows += len(rows)
                        job.progress = round(job.processed_rows / total_rows * 100, 2)
                        self.job_manager.update_job(job)

                    offset += config.batch_size

            job.status = JobStatus.COMPLETED
            job.progress = 100.0
            self.job_manager.update_job(job)

        except Exception as e:
            job.status = JobStatus.FAILED
            job.errors.append(str(e))
            self.job_manager.update_job(job)
