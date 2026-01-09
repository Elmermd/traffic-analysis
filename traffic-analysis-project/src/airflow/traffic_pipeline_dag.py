from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


# Default arguments
# El DAG define retries automáticos y dependencias estrictas.
#Si una tarea falla, Airflow la reintenta y bloquea la ejecución de tareas downstream, evitando resultados inconsistentes.
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
}

# DAG definition
with DAG(
    dag_id="nyc_traffic_2020_pipeline",
    description="NYC traffic analysis pipeline (TMAS + Weather + Incidents)",
    default_args=default_args,
    start_date=datetime(2020, 1, 1),
    schedule_interval=None,   # ejecución manual
    catchup=False,
    tags=["traffic", "nyc", "spark"],
) as dag:

    # Extract tasks
    extract_traffic = BashOperator(
        task_id="extract_traffic",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/extract/extract_tmas_ny_2020.py
        """,
            )

    extract_weather = BashOperator(
        task_id="extract_weather",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/extract/extract_weather_ny_2020.py
        """,
    )

    extract_incidents = BashOperator(
        task_id="extract_incidents",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/extract/extract_nyc_collisions_2020.py
        """,
    )

    # Transform (clean)
    clean_traffic = BashOperator(
        task_id="clean_traffic",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/transform/clean_traffic.py
        """,
    )

    clean_stations = BashOperator(
        task_id="clean_stations",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/transform/clean_stations.py
        """,
    )

    clean_weather = BashOperator(
        task_id="clean_weather",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/transform/clean_weather.py
        """,
    )

    clean_incidents = BashOperator(
        task_id="clean_incidents",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/transform/clean_incidents.py
        """,
    )

    # Integrations
    join_traffic_stations = BashOperator(
        task_id="join_traffic_stations",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/transform/integrate/join_traffic_stations.py
        """,
    )

    join_traffic_weather = BashOperator(
        task_id="join_traffic_weather",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/transform/integrate/join_traffic_weather.py
        """,
    )

    join_final = BashOperator(
        task_id="join_traffic_weather_incidents",
        bash_command="""
        cd /home/elias/Escritorio/traffic-analysis &&
        python src/transform/integrate/join_traffic_weather_incidents.py
        """,
    )

    # Dependencies
    extract_traffic >> clean_traffic
    extract_weather >> clean_weather
    extract_incidents >> clean_incidents

    [clean_stations, clean_traffic] >> join_traffic_stations
    join_traffic_stations >> join_traffic_weather
    clean_weather >> join_traffic_weather

    join_traffic_weather >> join_final
    clean_incidents >> join_final