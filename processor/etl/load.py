# etl/load.py
import os
import time
import tempfile

def upload_dataframe_to_stage(df, label, stage_name, run_time, file_format="csv"):
    from utils.connections import get_snowflake_connection

    filename = f"{label}_{run_time.strftime('%Y%m%d_%H%M%S')}.{file_format}"
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, filename)

        if file_format == "csv":
            df.to_csv(file_path, index=False, na_rep='')
        elif file_format == "json":
            df.to_json(file_path, orient="records", lines=True)
        else:
            raise ValueError("Unsupported file format for Snowflake upload.")

        print(f"Uploading {filename} to Snowflake stage {stage_name}")

        SNOWFLAKE_SCHEMA=os.getenv("SNOWFLAKE_SCHEMA")

        conn = get_snowflake_connection()
        cs = conn.cursor()
        try:
            cs.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA};")
            cs.execute(f"CREATE STAGE IF NOT EXISTS {stage_name};")
            cs.execute(f"PUT file://{file_path} @{stage_name}/ OVERWRITE = TRUE")
        finally:
            cs.close()
            conn.close()


def copy_stage_to_table(stage_name, table_name, file_format="CSV", connection=None):
    from utils.connections import get_snowflake_connection
    import os

    start = time.time()
    conn = connection or get_snowflake_connection()
    cs = conn.cursor()

    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

    try:
        if file_format.upper() == "CSV":
            format_options = """
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    SKIP_HEADER = 1
                    NULL_IF = ('')
                )
            """
        elif file_format.upper() == "JSON":
            format_options = """
                FILE_FORMAT = (
                    TYPE = 'JSON'
                    STRIP_OUTER_ARRAY = FALSE
                )
            """
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        sql = f"""
            COPY INTO {SNOWFLAKE_SCHEMA}.{table_name}
            FROM @{SNOWFLAKE_SCHEMA}.{stage_name}/
            {format_options}
            ON_ERROR = 'CONTINUE'
        """
        cs.execute(sql)
        results = cs.fetchall()

        rows_copied = sum(row[3] for row in results if row[3] is not None)
        rows_skipped = sum(row[4] for row in results if row[4] is not None)

        return {
            "rows_copied": rows_copied,
            "rows_skipped": rows_skipped,
            "execution_time_sec": round(time.time() - start, 2),
            "status": "success",
            "error_message": None
        }

    except Exception as e:
        return {
            "rows_copied": 0,
            "rows_skipped": 0,
            "execution_time_sec": round(time.time() - start, 2),
            "status": "error",
            "error_message": str(e)
        }
    finally:
        cs.close()
        if not connection:  # only close if we opened it
            conn.close()


def clean_stage(stage_name, connection=None):
    from utils.connections import get_snowflake_connection
    import os

    start = time.time()
    conn = connection or get_snowflake_connection()
    cs = conn.cursor()

    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

    try:
        cs.execute(f"REMOVE @{SNOWFLAKE_SCHEMA}.{stage_name}/")
        results = cs.fetchall()

        return {
            "files_removed": len(results),
            "execution_time_sec": round(time.time() - start, 2),
            "status": "success",
            "error_message": None
        }

    except Exception as e:
        return {
            "files_removed": 0,
            "execution_time_sec": round(time.time() - start, 2),
            "status": "error",
            "error_message": str(e)
        }
    finally:
        cs.close()
        if not connection:
            conn.close()