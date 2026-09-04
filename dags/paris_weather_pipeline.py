from datetime import datetime, timedelta

from airflow.sdk import DAG, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# -------------------------
# Push XCom Status
# -------------------------
def push_weather_status():

    context = get_current_context()
    ti = context["ti"]

    ti.xcom_push(
        key="weather_status",
        value="Weather pipeline completed successfully"
    )

    print("XCom pushed successfully")


# -------------------------
# Pull XCom Status
# -------------------------
def pull_weather_status():

    context = get_current_context()
    ti = context["ti"]

    status = ti.xcom_pull(
        task_ids="push_weather_status",
        key="weather_status"
    )

    print(f"Received XCom: {status}")


# -------------------------
# Load Data to PostgreSQL
# -------------------------
def load_weather():

    import pandas as pd

    file_path = "/opt/airflow/data/paris_weather_clean.csv"

    df = pd.read_csv(file_path)


    hook = PostgresHook(
        postgres_conn_id="postgres_weather"
    )


    conn = hook.get_conn()
    cursor = conn.cursor()


    sql = """
        INSERT INTO weather_data
        (
            timestamp,
            city,
            country,
            temperature_c,
            humidity_percent,
            wind_speed_kmh
        )

        VALUES
        (%s,%s,%s,%s,%s,%s)

        ON CONFLICT(city,timestamp)

        DO UPDATE SET

        country = EXCLUDED.country,
        temperature_c = EXCLUDED.temperature_c,
        humidity_percent = EXCLUDED.humidity_percent,
        wind_speed_kmh = EXCLUDED.wind_speed_kmh;
    """


    rows = [

        (
            row.timestamp,
            row.city,
            row.country,
            row.temperature_c,
            row.humidity_percent,
            row.wind_speed_kmh
        )

        for row in df.itertuples(index=False)

    ]


    try:

        cursor.executemany(
            sql,
            rows
        )

        conn.commit()

        print(
            f"Loaded {len(rows)} rows successfully"
        )


    except Exception as error:

        conn.rollback()

        print(
            f"Loading failed: {error}"
        )

        raise


    finally:

        cursor.close()
        conn.close()



# -------------------------
# Verify Database
# -------------------------
def verify_database():

    hook = PostgresHook(
        postgres_conn_id="postgres_weather"
    )


    result = hook.get_records(
        "SELECT COUNT(*) FROM weather_data"
    )


    total_rows = result[0][0]


    print(
        f"Total rows in database: {total_rows}"
    )



# -------------------------
# DAG
# -------------------------

default_args = {

    "retries": 2,

    "retry_delay": timedelta(minutes=5)

}



with DAG(

    dag_id="paris_weather_pipeline",

    start_date=datetime(2026, 9, 1),

    schedule="@daily",

    catchup=False,

    default_args=default_args,

    tags=[
        "weather",
        "paris",
        "etl"
    ]

) as dag:



    extract_task = BashOperator(

        task_id="extract_weather",

        bash_command=
        "python /opt/airflow/scripts/extract_weather.py"

    )



    transform = BashOperator(

        task_id="transform_weather",

        bash_command=
        "python /opt/airflow/scripts/transform_weather.py"

    )



    validate = BashOperator(

        task_id="validate_weather",

        bash_command=
        "python /opt/airflow/scripts/validate_weather.py"

    )



    load = PythonOperator(

        task_id="load_weather",

        python_callable=load_weather

    )



    push_status = PythonOperator(

        task_id="push_weather_status",

        python_callable=push_weather_status

    )



    pull_status = PythonOperator(

        task_id="pull_weather_status",

        python_callable=pull_weather_status

    )



    verify_db = PythonOperator(

        task_id="verify_database",

        python_callable=verify_database

    )



    # Pipeline Dependency

    extract_task >> transform >> validate >> load >> push_status >> pull_status >> verify_db