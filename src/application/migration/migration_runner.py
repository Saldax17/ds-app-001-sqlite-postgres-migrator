from src.application.migration.table_migrator import TableMigrator
from src.application.schema.type_converter import TypeConverter
from src.domain.models import JobStatus

from src.infrastructure.sqlite_reader import SQLiteReader
from src.infrastructure.postgres_writer import PostgreSQLWriter


class MigrationRunner:
    def __init__(self, job_manager):
        self.job_manager = job_manager

    # =====================================================
    # PUBLIC – RUN FULL MIGRATION
    # =====================================================
    def run(self, job_id: str, req) -> None:
        job = self.job_manager.get(job_id)

        reader = SQLiteReader(req.sqlite_path)
        writer = PostgreSQLWriter(req.postgres_url)
        converter = TypeConverter()

        try:
            tables = reader.get_tables()
            job.total_rows = self._count_total_rows(reader, tables)
            self.job_manager.update(job)

            table_migrator = TableMigrator(
                reader=reader,
                writer=writer,
                converter=converter,
                job_manager=self.job_manager,
            )

            for table in tables:
                if job.status == JobStatus.CANCELLED:
                    return

                table_migrator.migrate(
                    job=job,
                    table=table,
                    batch_size=req.batch_size,
                )

            job.mark_completed()
            self.job_manager.update(job)

        except Exception as e:
            self._fail_job(job, str(e))

    # =====================================================
    # INTERNAL – JOB FAILURE
    # =====================================================
    def _fail_job(self, job, error: str):
        job.mark_failed(error)
        self.job_manager.update(job)

    # =====================================================
    # INTERNAL – ROW COUNT
    # =====================================================
    def _count_total_rows(self, reader, tables) -> int:
        total = 0
        for table in tables:
            total += self._count_rows(reader, table)
        return total

    def _count_rows(self, reader, table: str) -> int:
        conn = reader.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            return cursor.fetchone()[0]
        finally:
            conn.close()
