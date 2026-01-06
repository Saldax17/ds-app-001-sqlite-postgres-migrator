from src.application.schema.schema_validator import SchemaValidator
from src.application.migration.sql_builder import SQLBuilder
from src.domain.models import JobStatus


class TableMigrator:
    def __init__(
        self,
        reader,
        writer,
        converter,
        job_manager,
    ):
        self.reader = reader
        self.writer = writer
        self.converter = converter
        self.job_manager = job_manager
        self.schema_validator = SchemaValidator()

    # =====================================================
    # PUBLIC – MIGRATE SINGLE TABLE
    # =====================================================
    def migrate(self, job, table: str, batch_size: int) -> None:
        sqlite_schema = self._get_sqlite_schema(table)
        postgres_schema = self._get_postgres_schema(table)

        self._validate_schema(sqlite_schema, postgres_schema, table)
        self._create_table_if_needed(table, sqlite_schema)

        self._insert_batches(
            job=job,
            table=table,
            schema=sqlite_schema,
            batch_size=batch_size,
        )

    # =====================================================
    # INTERNAL – SCHEMA
    # =====================================================
    def _get_sqlite_schema(self, table: str):
        return self.reader.get_table_schema(table)

    def _get_postgres_schema(self, table: str):
        return self.writer.get_table_schema(table)

    def _validate_schema(self, sqlite_schema, postgres_schema, table: str):
        result = self.schema_validator.validate(
            sqlite_schema=sqlite_schema,
            postgres_schema=postgres_schema,
            table_name=table,
        )

        if not result.valid:
            raise ValueError("; ".join(result.errors))

    # =====================================================
    # INTERNAL – TABLE CREATION
    # =====================================================
    def _create_table_if_needed(self, table: str, schema):
        sql = SQLBuilder.build_create_table(
            table=table,
            schema=schema,
            converter=self.converter,
        )
        self.writer.execute_sql(sql)

    # =====================================================
    # INTERNAL – DATA INSERTION
    # =====================================================
    def _insert_batches(
        self,
        job,
        table: str,
        schema,
        batch_size: int,
    ):
        for batch in self.reader.read_rows(table, batch_size):
            if job.status == JobStatus.CANCELLED:
                return

            self.writer.insert_rows(
                table=table,
                columns=[c["name"] for c in schema],
                rows=batch,
            )

            self._update_progress(job, len(batch))

    # =====================================================
    # INTERNAL – JOB PROGRESS
    # =====================================================
    def _update_progress(self, job, processed_rows: int):
        job.processed_rows += processed_rows
        job.progress = round(
            (job.processed_rows / job.total_rows) * 100, 2
        )
        self.job_manager.update(job)

