"""
## Toy example of using the PythonVirtualenvOperator

The PythonVirtualenvOperator is used to run any Python code in a new
isolated Python environment. It is the traditional operator verision of the @task.virtualenv decorator.
There is the option to cache the environment for future runs.

The DAG shows how to use the PythonVirtualenvOperator and how to pass
[XCom](https://docs.astronomer.io/learn/airflow-passing-data-between-tasks) in an out of the task.

Learn more:
- [Run tasks in an isolated environment in Apache Airflow](https://docs.astronomer.io/learn/airflow-isolated-environments) guide.
- [PythonVirtualenvOperator full list of parameters](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/PythonVirtualenvOperator)
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonVirtualenvOperator
import pandas as pd
import sys


def my_isolated_function(num: int, word: str, logical_date_from_op_kwargs: str) -> dict:
    """
    This function will be passed to the PythonVirtualenvOperator to
    run in an isolated environment.
    Args:
        num (int): An integer to be incremented by 1.
        word (str): A string to have an exclamation mark added to it.
        logical_date_from_op_kwargs (str): The logical_date of the DAG.
    Returns:
        pd.DataFrame: A dictionary containing the transformed inputs.
    """
    import pandas as pd
    import sys

    print(f"The python version in the virtual env is: {sys.version}")
    print(f"The pandas version in the virtual env is: {pd.__version__}")
    print(f"The logical_date is {logical_date_from_op_kwargs}")

    num_plus_one = num + 1
    word_plus_exclamation = word + "!"

    df = pd.DataFrame(
        {
            "num_plus_one": [num_plus_one],
            "word_plus_explamation": [word_plus_exclamation],
        },
    )

    return df


@dag(
    start_date=None,
    schedule=None,
    doc_md=__doc__,
    description="PythonVirtualenvOperator",
    render_template_as_native_obj=True,
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["PythonVirtualenvOperator"],
)
def python_virtualenv_operator_dag():

    @task
    def upstream_task():
        print(f"The python version in the upstream task is: {sys.version}")
        print(f"The pandas version in the upstream task is: {pd.__version__}")
        return {"num": 1, "word": "hello"}

    my_isolated_task = PythonVirtualenvOperator(
        task_id="my_isolated_task",
        python_callable=my_isolated_function,
        requirements=[
            "pandas==1.5.1",
            "pendulum==3.0.0",
        ],  # pendulum is needed to use the logical date
        python_version="3.10",
        op_kwargs={
            "logical_date_from_op_kwargs": "{{ logical_date }}",
            "num": "{{ ti.xcom_pull(task_ids='upstream_task')['num']}}",  # note that render_template_as_native_obj=True in the DAG definition
            "word": "{{ ti.xcom_pull(task_ids='upstream_task')['word']}}",
        },
    )

    @task
    def downstream_task(arg):
        print(f"The python version in the downstream task is: {sys.version}")
        print(f"The pandas version in the downstream task is: {pd.__version__}")
        return arg

    chain(upstream_task(), my_isolated_task, downstream_task(my_isolated_task.output))


python_virtualenv_operator_dag()
