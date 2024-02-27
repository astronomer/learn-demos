
"""
## Example of using PythonVirtualenvOperator with templated requirements

This example demonstrates how to use the PythonVirtualenvOperator with templated requirements, to
change the version of a package based on the result of a previous task.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import sys
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonVirtualenvOperator


def my_isolated_function():
    import pandas as pd

    print(f"The python version in the virtual env is: {sys.version}")
    print(f"The pandas version in the virtual env is: {pd.__version__}")


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
def templated_virtual_env():

    @task
    def get_pandas_version():
        pandas_version = "1.5.1"  # retrieve the pandas version according to your logic
        return pandas_version

    my_isolated_task_1 = PythonVirtualenvOperator(
        task_id="my_isolated_task2",
        python_callable=my_isolated_function,
        requirements=[
            "pandas=={{ ti.xcom_pull(task_ids='get_pandas_version') }}",
        ],
    )

    chain(get_pandas_version(), my_isolated_task_1)


templated_virtual_env()
