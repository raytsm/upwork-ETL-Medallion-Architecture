from airflow.sdk import dag, task
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.utils.helpers import file_hash
from pathlib import Path

load_dotenv()

POSTGRES_CONN_ID = os.getenv("CONNECTION_ID")
# print(POSTGRES_CONN_ID)
DATABASE = os.getenv("DATABASE")
# print(DATABASE)

@dag(
    start_date=datetime(2026, 10, 1, 9),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30)
    },
    tags=["upwork", "ELT", "medallionArch"]
)
def upwork_dag():
    @task
    def initialise() -> None:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.ingestion_batches (
                source_hash VARCHAR PRIMARY KEY,
                source_file VARCHAR NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.upwork_raw_jobs (
                id VARCHAR,
                url VARCHAR,
                title VARCHAR,
                description VARCHAR,
                budget VARCHAR,
                budget_type VARCHAR,
                hourly_rate_min VARCHAR,
                hourly_rate_max VARCHAR,
                work_type VARCHAR,
                experience_level VARCHAR,
                duration VARCHAR,
                location VARCHAR,
                category VARCHAR,
                skills VARCHAR,
                proposals VARCHAR,
                client_info VARCHAR,
                posted_date VARCHAR,
                crawled_date VARCHAR,
                raw_data VARCHAR,
                created_at VARCHAR,
                updated_at VARCHAR,
                source_file VARCHAR NOT NULL,
                source_hash VARCHAR NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()

    @task
    def ingest_bronze(source: Path) -> bool:
        """ 
        Bronze Layer: unfiltered data records, normally immutable and append only 
        Include ingestion metadata alongside the source fields such as
        - source system, file or event identifier
        - ingestion timestamps and/or business effective data
        - number of records ingested
        - batch or loading run identifier
        - should be idempotent, append only using file hash, batch identifier or source key to prevent accidental duplication during interruption/failure
        """
        source = Path(source).resolve()

        if not source.is_file():
            raise FileNotFoundError(f"CSV not found: {source}")
        
        digest = file_hash(source)

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO bronze.ingestion_batches
                    (source_hash, source_file)
                VALUES (%s, %s)
                ON CONFLICT (source_hash) DO NOTHING
                RETURNING source_hash
                """,
                (digest, source.name),
            )

            if cursor.fetchone() is None:
                print(f"Skipping{source.name}: already loaded")
                conn.rollback()
                return False

            cursor.execute(
                """
                CREATE TEMP TABLE upwork_stage
                ON COMMIT DROP AS
                SELECT
                    id, url, title, description,
                    budget, budget_type,
                    hourly_rate_min, hourly_rate_max,
                    work_type, experience_level,
                    duration, location, category,
                    skills, proposals, client_info,
                    posted_date, crawled_date,
                    raw_data, created_at, updated_at
                FROM bronze.upwork_raw_jobs
                WITH NO DATA
                """
            )
            copy_sql = """
                COPY upwork_stage
                FROM STDIN
                WITH (FORMAT CSV, HEADER TRUE)
            """

            with source.open("rb") as file:
                if hasattr(cursor, "copy"):
                    with cursor.copy(copy_sql) as copy:
                        while chunk := file.read(1024 * 1024):
                            copy.write(chunk)
                else:
                    cursor.copy_expert(copy_sql, file)

            cursor.execute(
                """
                INSERT INTO bronze.upwork_raw_jobs
                SELECT *, %s, %s, CURRENT_TIMESTAMP
                FROM upwork_stage
                """,
                (source.name, digest),
            )

            conn.commit()
            print(f"Loaded {source.name} into bronze")
            return True
        
        except Exception:
            conn.rollback()
            raise

        finally:
            cursor.close()
            conn.close()



    @task
    def build_silver() -> None:
        """ 
        Silver Layer: remove or remediate null records, invalid dates, missing fields, etc before being stored
        - parsing and enforcing data types
        - standardisiing dates, currencies, country codes and units
        - deduplicating records
        - quaranting duff data
        - joining reference data
        usually in 3NF 
        Two Tables: types, validated and deduplicated records on one & rejected records and reason on another quarantine
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        valid = """
            id IS NOT NULL
            AND url IS NOT NULL
            AND url ~ '^https://www[.]upwork[.]com/jobs/'
            and title IS NOT NULL
            AND posted_date IS NOT NULL
            AND crawled_date IS NOT NULL
            AND budget_type IN ('fixed', 'hourly')
            AND experience_level IN ('ENTRY_LEVEL', 'INTERMEDIATE', 'EXPERT')
            AND (budget IS NULL OR budget >= 0)
            AND (hourly_rate_min IS NOT NULL OR hourly_rate_min >= 0)
            AND (hourly_rate_max IS NULL OR hourly_rate_max >= 0)
            AND (
                hourly_rate_min IS NULL
                OR hourly_rate_max IS NULL
                OR hourly_rate_min <= hourly_rate_max
            )
            AND (proposals IS NULL OR proposals >= 0)
            AND skills IS NOT NULL
            AND client_info IS NOT NULL
            AND raw_data IS NOT NULL
            AND duplicate_rank = 1
        """

        sql = f"""
            CREATE SCHEMA IF NOT EXISTS silver;
        """

    # @task
    # def aggregate_data():
    #     """ 
    #     Gold Layer: organise for particular business use cases and processes
    #     - Summarised and aggregated data sets such as totals and counts by day, month, or region(e.g., total sales, active users).
    #     - Star schemas or data marts built for fast queries with fewer joins
    #     - Tailored, separate datasets for specific teams like finance, marketing or operations
    #     - make a graph on gold summarization/aggregation like jobs for some tile, and a graph on how information flow
    #     """
    #     pass

    setup = initialise()
    bronze = ingest_bronze(Path("include/data/upwork_raw_jobs.csv"))
    setup >> bronze

upwork_dag()