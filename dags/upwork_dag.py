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
        """Normalize, type, and deduplicate Bronze jobs; quarantine rejected rows."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = None

        valid = """
            id IS NOT NULL
            AND url IS NOT NULL
            AND url ~ '^https://www[.]upwork[.]com/jobs/'
            AND title IS NOT NULL
            AND posted_date IS NOT NULL
            AND crawled_date IS NOT NULL
            AND budget_type IN ('fixed', 'hourly')
            AND experience_level IN (
                'ENTRY_LEVEL', 'INTERMEDIATE', 'EXPERT'
            )
            AND (budget IS NULL OR budget >= 0)
            AND (hourly_rate_min IS NULL OR hourly_rate_min >= 0)
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
            AND NOT invalid_conversion
            AND duplicate_rank = 1
        """

        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TEMP TABLE normalized_jobs
                ON COMMIT DROP
                AS
                SELECT
                    NULLIF(UPPER(TRIM(id)), 'NULL') AS id,
                    NULLIF(TRIM(url), '') AS url,
                    NULLIF(TRIM(title), '') AS title,
                    NULLIF(description, 'NULL') AS description,
                    NULLIF(UPPER(TRIM(budget)), 'NULL') AS budget,
                    LOWER(NULLIF(TRIM(budget_type), '')) AS budget_type,
                    NULLIF(UPPER(TRIM(hourly_rate_min)), 'NULL') AS hourly_rate_min,
                    NULLIF(UPPER(TRIM(hourly_rate_max)), 'NULL') AS hourly_rate_max,
                    NULLIF(TRIM(work_type), '') AS work_type,
                    UPPER(NULLIF(TRIM(experience_level), '')) AS experience_level,
                    NULLIF(TRIM(duration), '') AS duration,
                    NULLIF(TRIM(location), '') AS location,
                    NULLIF(TRIM(category), '') AS category,
                    NULLIF(TRIM(skills), '') AS skills,
                    NULLIF(UPPER(TRIM(proposals)), 'NULL') AS proposals,
                    NULLIF(TRIM(client_info), '') AS client_info,
                    NULLIF(TRIM(raw_data), '') AS raw_data,
                    NULLIF(TRIM(posted_date), '') AS posted_date,
                    NULLIF(TRIM(crawled_date), '') AS crawled_date,
                    NULLIF(TRIM(created_at), '') AS created_at,
                    NULLIF(TRIM(updated_at), '') AS updated_at,
                    source_file,
                    source_hash,
                    ingested_at
                FROM bronze.upwork_raw_jobs;
            """)

            cursor.execute("""
                CREATE TEMP TABLE typed_jobs
                ON COMMIT DROP
                AS
                SELECT
                    CASE
                        WHEN pg_input_is_valid(id, 'bigint')
                        THEN id::BIGINT
                    END AS id,
                    url,
                    title,
                    description,
                    CASE
                        WHEN pg_input_is_valid(budget, 'numeric(18,2)')
                        THEN budget::NUMERIC(18,2)
                    END AS budget,
                    budget_type,
                    CASE
                        WHEN pg_input_is_valid(
                            hourly_rate_min, 'numeric(18,2)'
                        )
                        THEN hourly_rate_min::NUMERIC(18,2)
                    END AS hourly_rate_min,
                    CASE
                        WHEN pg_input_is_valid(
                            hourly_rate_max, 'numeric(18,2)'
                        )
                        THEN hourly_rate_max::NUMERIC(18,2)
                    END AS hourly_rate_max,
                    work_type,
                    experience_level,
                    duration,
                    location,
                    category,
                    CASE
                        WHEN pg_input_is_valid(skills, 'jsonb')
                        THEN skills::JSONB
                    END AS skills,
                    CASE
                        WHEN pg_input_is_valid(proposals, 'integer')
                        THEN proposals::INTEGER
                    END AS proposals,
                    CASE
                        WHEN pg_input_is_valid(client_info, 'jsonb')
                        THEN client_info::JSONB
                    END AS client_info,
                    CASE
                        WHEN pg_input_is_valid(raw_data, 'jsonb')
                        THEN raw_data::JSONB
                    END AS raw_data,
                    CASE
                        WHEN pg_input_is_valid(posted_date, 'timestamptz')
                        THEN posted_date::TIMESTAMPTZ
                    END AS posted_date,
                    CASE
                        WHEN pg_input_is_valid(crawled_date, 'timestamptz')
                        THEN crawled_date::TIMESTAMPTZ
                    END AS crawled_date,
                    CASE
                        WHEN pg_input_is_valid(created_at, 'timestamp')
                        THEN created_at::TIMESTAMP
                    END AS created_at,
                    CASE
                        WHEN pg_input_is_valid(updated_at, 'timestamp')
                        THEN updated_at::TIMESTAMP
                    END AS updated_at,
                    source_file,
                    source_hash,
                    ingested_at,
                    -- Preserve evidence of failed casts.
                    (
                        (id IS NOT NULL
                            AND NOT pg_input_is_valid(id, 'bigint'))

                        OR (budget IS NOT NULL
                            AND NOT pg_input_is_valid(
                                budget, 'numeric(18,2)'
                            ))

                        OR (hourly_rate_min IS NOT NULL
                            AND NOT pg_input_is_valid(
                                hourly_rate_min, 'numeric(18,2)'
                            ))

                        OR (hourly_rate_max IS NOT NULL
                            AND NOT pg_input_is_valid(
                                hourly_rate_max, 'numeric(18,2)'
                            ))

                        OR (proposals IS NOT NULL
                            AND NOT pg_input_is_valid(
                                proposals, 'integer'
                            ))

                        OR (posted_date IS NOT NULL
                            AND NOT pg_input_is_valid(
                                posted_date, 'timestamptz'
                            ))

                        OR (crawled_date IS NOT NULL
                            AND NOT pg_input_is_valid(
                                crawled_date, 'timestamptz'
                            ))

                        OR (skills IS NOT NULL
                            AND NOT pg_input_is_valid(skills, 'jsonb'))

                        OR (client_info IS NOT NULL
                            AND NOT pg_input_is_valid(
                                client_info, 'jsonb'
                            ))

                        OR (raw_data IS NOT NULL
                            AND NOT pg_input_is_valid(
                                raw_data, 'jsonb'
                            ))
                    ) AS invalid_conversion,
                    ROW_NUMBER() OVER (
                        PARTITION BY url
                        ORDER BY
                            CASE
                                WHEN pg_input_is_valid(
                                    crawled_date, 'timestamptz'
                                )
                                THEN crawled_date::TIMESTAMPTZ
                            END DESC NULLS LAST,
                            ingested_at DESC NULLS LAST,
                            source_file DESC NULLS LAST,
                            id DESC NULLS LAST
                    ) AS duplicate_rank

                FROM normalized_jobs;
            """)

            cursor.execute("CREATE SCHEMA IF NOT EXISTS silver;")
            cursor.execute("DROP TABLE IF EXISTS silver.upwork_jobs;")
            cursor.execute(f"""
                CREATE TABLE silver.upwork_jobs AS
                SELECT
                    id,
                    url,
                    title,
                    description,
                    budget,
                    budget_type,
                    hourly_rate_min,
                    hourly_rate_max,
                    work_type,
                    experience_level,
                    duration,
                    location,
                    category,
                    skills,
                    proposals,
                    client_info,
                    posted_date,
                    crawled_date,
                    raw_data,
                    created_at,
                    updated_at,
                    source_file,
                    source_hash,
                    ingested_at
                FROM typed_jobs
                WHERE ({valid}) IS TRUE;
            """)

            cursor.execute("DROP TABLE IF EXISTS silver.upwork_jobs_quarantine;")
            cursor.execute(f"""
                CREATE TABLE silver.upwork_jobs_quarantine AS
                SELECT
                    id,
                    url,
                    title,
                    description,
                    budget,
                    budget_type,
                    hourly_rate_min,
                    hourly_rate_max,
                    work_type,
                    experience_level,
                    duration,
                    location,
                    category,
                    skills,
                    proposals,
                    client_info,
                    posted_date,
                    crawled_date,
                    raw_data,
                    created_at,
                    updated_at,
                    source_file,
                    source_hash,
                    ingested_at,

                    CASE
                        WHEN duplicate_rank > 1
                            THEN 'duplicate job URL'

                        WHEN id IS NULL
                            THEN 'invalid id'

                        WHEN url IS NULL
                            OR url !~ '^https://www[.]upwork[.]com/jobs/'
                            THEN 'invalid URL'

                        WHEN title IS NULL
                            THEN 'missing title'

                        WHEN posted_date IS NULL
                            THEN 'invalid posted date'

                        WHEN crawled_date IS NULL
                            THEN 'invalid crawled date'

                        WHEN budget_type IS NULL
                            OR budget_type NOT IN ('fixed', 'hourly')
                            THEN 'invalid budget type'

                        WHEN experience_level IS NULL
                            OR experience_level NOT IN (
                                'ENTRY_LEVEL',
                                'INTERMEDIATE',
                                'EXPERT'
                            )
                            THEN 'invalid experience level'

                        WHEN budget < 0
                            THEN 'negative budget'

                        WHEN hourly_rate_min < 0
                            OR hourly_rate_max < 0
                            THEN 'negative hourly rate'

                        WHEN hourly_rate_min > hourly_rate_max
                            THEN 'invalid hourly rate range'

                        WHEN proposals < 0
                            THEN 'negative proposals'

                        WHEN skills IS NULL
                            THEN 'invalid skills'

                        WHEN client_info IS NULL
                            THEN 'invalid client information'

                        WHEN raw_data IS NULL
                            THEN 'invalid raw JSON'

                        WHEN invalid_conversion
                            THEN 'invalid data type'

                        ELSE 'other validation failure'

                    END AS rejection_reason

                FROM typed_jobs
                WHERE ({valid}) IS NOT TRUE;
            """)
            conn.commit()

        except Exception:
            conn.rollback()
            raise

        finally:
            if cursor is not None:
                cursor.close()

            conn.close()

    @task
    def build_gold() -> None:
        pg_hook = PostgresHook(
            postgres_conn_id=os.environ["CONNECTION_ID"]
        )

        conn = pg_hook.get_conn()
        cursor = None

        try:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS gold;
            """)

            # Jobs by Category
            cursor.execute("""
                DROP TABLE IF EXISTS gold.jobs_by_category;

                CREATE TABLE gold.jobs_by_category AS
                SELECT
                    COALESCE(category, 'Unknown') AS category,

                    COUNT(*) AS total_jobs,

                    COUNT(*) FILTER (
                        WHERE budget_type = 'fixed'
                    ) AS fixed_jobs,

                    COUNT(*) FILTER (
                        WHERE budget_type = 'hourly'
                    ) AS hourly_jobs

                FROM silver.upwork_jobs
                GROUP BY COALESCE(category, 'Unknown');
            """)

            #Fixed Budget Analysis
            cursor.execute("""
                DROP TABLE IF EXISTS gold.budget_analysis;

                CREATE TABLE gold.budget_analysis AS
                SELECT
                    COALESCE(category, 'Unknown') AS category,

                    COUNT(*) AS total_jobs,

                    ROUND(AVG(budget), 2) AS avg_budget,

                    MIN(budget) AS min_budget,

                    MAX(budget) AS max_budget,

                    ROUND(
                        (
                            PERCENTILE_CONT(0.5)
                            WITHIN GROUP (ORDER BY budget)
                        )::NUMERIC,
                        2
                    ) AS median_budget

                FROM silver.upwork_jobs
                WHERE budget_type = 'fixed'
                AND budget IS NOT NULL
                GROUP BY COALESCE(category, 'Unknown');
            """)

            # Hourly Rate Analysis
            cursor.execute("""
                DROP TABLE IF EXISTS gold.hourly_rate_analysis;

                CREATE TABLE gold.hourly_rate_analysis AS
                SELECT
                    COALESCE(category, 'Unknown') AS category,

                    COUNT(*) AS total_jobs,

                    COUNT(hourly_rate_min) AS jobs_with_min_rate,

                    COUNT(hourly_rate_max) AS jobs_with_max_rate,

                    ROUND(
                        AVG(hourly_rate_min), 2
                    ) AS avg_min_hourly_rate,

                    ROUND(
                        AVG(hourly_rate_max), 2
                    ) AS avg_max_hourly_rate,

                    MIN(hourly_rate_min) AS lowest_hourly_rate,

                    MAX(hourly_rate_max) AS highest_hourly_rate

                FROM silver.upwork_jobs
                WHERE budget_type = 'hourly'
                GROUP BY COALESCE(category, 'Unknown');
            """)

            # Jobs by Experience Level
            cursor.execute("""
                DROP TABLE IF EXISTS gold.jobs_by_experience;

                CREATE TABLE gold.jobs_by_experience AS
                SELECT
                    experience_level,

                    COUNT(*) AS total_jobs,

                    COUNT(*) FILTER (
                        WHERE budget_type = 'fixed'
                    ) AS fixed_jobs,

                    COUNT(*) FILTER (
                        WHERE budget_type = 'hourly'
                    ) AS hourly_jobs,

                    ROUND(
                        AVG(budget) FILTER (
                            WHERE budget_type = 'fixed'
                        ), 2
                    ) AS avg_fixed_budget,

                    ROUND(
                        AVG(hourly_rate_min) FILTER (
                            WHERE budget_type = 'hourly'
                        ), 2
                    ) AS avg_min_hourly_rate,

                    ROUND(
                        AVG(hourly_rate_max) FILTER (
                            WHERE budget_type = 'hourly'
                        ), 2
                    ) AS avg_max_hourly_rate

                FROM silver.upwork_jobs
                GROUP BY experience_level;
            """)

            # Daily Job Trends
            cursor.execute("""
                DROP TABLE IF EXISTS gold.daily_job_trends;

                CREATE TABLE gold.daily_job_trends AS
                SELECT
                    (posted_date AT TIME ZONE 'UTC')::DATE
                        AS posted_day,

                    COUNT(*) AS total_jobs,

                    COUNT(*) FILTER (
                        WHERE budget_type = 'fixed'
                    ) AS fixed_jobs,

                    COUNT(*) FILTER (
                        WHERE budget_type = 'hourly'
                    ) AS hourly_jobs,

                    COUNT(DISTINCT category) AS total_categories

                FROM silver.upwork_jobs

                GROUP BY
                    (posted_date AT TIME ZONE 'UTC')::DATE

                ORDER BY posted_day;
            """)
            conn.commit()

        except Exception:
            conn.rollback()
            raise

        finally:
            if cursor is not None:
                cursor.close()
            conn.close()

    setup = initialise()
    bronze = ingest_bronze(Path("include/data/upwork_500_data.csv"))
    silver = build_silver()
    gold = build_gold()
    setup >> bronze >> silver >> gold

upwork_dag()
