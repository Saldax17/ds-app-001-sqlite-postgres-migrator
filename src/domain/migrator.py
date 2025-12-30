import time
from typing import Optional

from src.adapters.sqlite_reader import SQLiteReader
from src.adapters.postgres_writer import PostgreSQLWriter
from src.adapters.type_converter import TypeConverter
from src.infrastructure.job_manager import JobManager
from src.domain.models import JobStatus, MigrationJob


class Migrator:

    def __init__(
        self,
        sqlite_path: str,
        postgres_url: str,
        batch_size: int = 1000
    ):
        self.sqlite_reader = SQLiteReader(sqlite_path)
        self.postgres_writer = PostgreSQLWriter(postgres_url)
        self.converter = TypeConverter()
        self.job_manager = JobManager()

        self.batch_size = batch_size

    # =====================================================
    # MAIN ENTRYPOINT – RUN MIGRATION
    # =====================================================
    def run(self, job_id: str):
        job = self.job_manager.get_job(job_id)

        try:
            job.status = JobStatus.RUNNING
            self.job_manager.update_job(job)

            tables = self.sqlite_reader.get_tables()
            job.total_rows = 0
            job.processed_rows = 0

            # Count rows for progress estimation
            for table in tables:
                job.total_rows += self._count_rows(table)

            self.job_manager.update_job(job)

            # Process table by table
            for table in tables:
                self._migrate_table(job, table)

            job.status = JobStatus.COMPLETED
            self.job_manager.update_job(job)

        except Exception as e:
            job.status = JobStatus.FAILED
            job.errors.append(str(e))
            self.job_manager.update_job(job)

            raise

    # =====================================================
    # COUNT ROWS FOR PROGRESS PERCENTAGE
    # =====================================================
    def _count_rows(self, table: str) -> int:
        conn = self.sqlite_reader.connect()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total = cursor.fetchone()[0]
        conn.close()
        return total

    # =====================================================
    # MIGRATE A SINGLE TABLE
    # =====================================================
    def _migrate_table(self, job: MigrationJob, table: str):
        # Extract schema
        schema = self.sqlite_reader.get_table_schema(table)

        # Create table in PostgreSQL
        self.postgres_writer.create_table(
            table_name=table,
            schema=schema,
            converter=self.converter
        )

        # Stream rows and insert into PostgreSQL
        for batch in self.sqlite_reader.read_rows(table, self.batch_size):

            if job.status == JobStatus.CANCELLED:
                return

            self.postgres_writer.insert_rows(table, schema, batch)

            # Update job progress
            job.processed_rows += len(batch)
            job.progress = round((job.processed_rows / job.total_rows) * 100, 2)
            self.job_manager.update_job(job)

            time.sleep(0.01)  # Simulate async worker pacing
