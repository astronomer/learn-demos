"""
## Example of using a secret stored in an Airflow variable in a Python Virtual Environment

To use this DAG you will need to define an Airflow variable called `my_secret`.
Learn more about [Airflow Variables](https://docs.astronomer.io/learn/airflow-variables).
"""

from airflow.decorators import dag
from pendulum import datetime
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.python import ExternalPythonOperator
import os


def my_isolated_function(password_from_op_kwargs):
    print(f"The password is: {password_from_op_kwargs}")


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
def use_secret():

    PythonVirtualenvOperator(
        task_id="my_isolated_task_p",
        python_callable=my_isolated_function,
        requirements=[
            "pandas==1.5.1",
        ],
        python_version="3.10",
        op_kwargs={
            "password_from_op_kwargs": "{{ var.value.my_secret }}",
        },
    )

    ExternalPythonOperator(
        task_id="my_isolated_task_e",
        python_callable=my_isolated_function,
        python=os.environ["ASTRO_PYENV_epo_pyenv"],
        op_kwargs={
            "password_from_op_kwargs": "{{ var.value.my_secret }}",
        },
    )


use_secret()
