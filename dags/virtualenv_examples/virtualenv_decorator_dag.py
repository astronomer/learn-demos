"""
## Toy example of using the @task.virtualenv decorator

The @task.virtualenv decorator is used to run any Python code in a new
isolated Python environment.
There is the option to cache the environment for future runs.

The DAG shows how to use the @task.virtualenv decorator and how to pass
[XCom](https://docs.astronomer.io/learn/airflow-passing-data-between-tasks) in an out of the task.

Learn more:
- [Run tasks in an isolated environment in Apache Airflow](https://docs.astronomer.io/learn/airflow-isolated-environments) guide.
- [PythonVirtualenvOperator full list of parameters](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/PythonVirtualenvOperator)
"""

from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
import sys


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
def virtualenv_decorator_dag():

    @task
    def upstream_task():
        print(f"The python version in the upstream task is: {sys.version}")
        print(f"The pandas version in the upstream task is: {pd.__version__}")
        return {"num": 1, "word": "hello"}

    @task.virtualenv(
        requirements=[
            "pandas==1.5.1",
            "pendulum==3.0.0",
        ],  # pendulum is needed to use the logical date
        python_version="3.10",
    )
    def my_isolated_task(
        upstream_task_output: dict, logical_date
    ):  # note that not all objects from the context can be used!
        """
        This function runs in an isolated environment.
        Args:
            upstream_task_output (dict): contains a number and a word.
            logical_date: The logical_date of the DAG.
        Returns:
            pd.DataFrame: A dictionary containing the transformed inputs.
        """
        import pandas as pd
        import sys

        print(f"The python version in the virtual env is: {sys.version}")
        print(f"The pandas version in the virtual env is: {pd.__version__}")
        print(f"The logical_date is {logical_date}")

        num = upstream_task_output["num"]
        word = upstream_task_output["word"]

        num_plus_one = num + 1
        word_plus_exclamation = word + "!"

        df = pd.DataFrame(
            {
                "num_plus_one": [num_plus_one],
                "word_plus_exclamation": [word_plus_exclamation],
            },
        )

        return df

    @task
    def downstream_task(arg):
        print(f"The python version in the downstream task is: {sys.version}")
        print(f"The pandas version in the downstream task is: {pd.__version__}")
        return arg

    downstream_task(my_isolated_task(upstream_task_output=upstream_task()))


virtualenv_decorator_dag()
