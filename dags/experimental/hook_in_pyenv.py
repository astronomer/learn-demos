"""
## Example of using a Hook within a Python Virtual Environment

Warning: Using Airflow packages inside of isolated environments can lead to unexpected behavior and is not recommended.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    doc_md=__doc__,
    description="@task.virtualenv",
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["@task.virtualenv"],
)
def hook_in_pyenv():

    @task.virtualenv(
        requirements=[
            "apache-airflow-providers-snowflake==5.3.0", # This is not recommended
            "apache-airflow==2.8.2+astro.1",  # This is not recommended
            "pandas==1.5.3",
        ],
        venv_cache_path="/tmp/venv_cache",
    )
    def my_isolated_task():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        import pandas as pd

        hook = SnowflakeHook(snowflake_conn_id="snowflake_de_team")

        result = hook.get_first("SELECT * FROM SALES_REPORTS_TABLE LIMIT 1")

        print(f"The pandas version in the virtual env is: {pd.__version__}")

        return result

    my_isolated_task()


hook_in_pyenv()
