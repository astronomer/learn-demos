"""
## Toy example of using the @task.branch_virtualenv decorator

The @task.branch_virtualenv decorator is used to run any Python code in an
newly created isolated Python environment to create conditional logic.
The task_id or list of task_ids returned by the function will be used to decide which downstream task(s) to run.

Learn more: 
- [Run tasks in an isolated environment in Apache Airflow](https://docs.astronomer.io/learn/airflow-isolated-environments) guide.
- [BranchPythonVirtualenvOperator full list of parameters](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/BranchPythonVirtualenvOperator)
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models.baseoperator import chain
import random
import pandas as pd


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    doc_md=__doc__,
    description="@task.branch_virtualenv",
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["@task.branch_virtualenv"],
)
def branching_with_virtualenv_dag():

    @task
    def upstream_task():

        print(f"The pandas version in the upstream task is: {pd.__version__}")

        num1 = random.randint(1, 100)
        num2 = random.randint(1, 100)
        num3 = random.randint(1, 100)

        df = pd.DataFrame({"num": [num1, num2, num3]})

        return df

    @task.branch_virtualenv(requirements=["pandas==1.5.3"])
    def my_isolated_task(df):
        """
        This function runs in an isolated environment to decide which downstream task to run.
        Args:
            df (pd.DataFrame): contains a column with 3 numbers.
        Returns:
            str: The task_id of the downstream task to run.
        """
        import pandas as pd

        print(f"The pandas version in the virtual env is: {pd.__version__}")

        summed_nums = df["num"].sum()

        if summed_nums > 150:
            return "downstream_task_a"
        else:
            return "downstream_task_b"

    @task
    def downstream_task_a():
        return "Hi, I'm the downstream task A."

    @task
    def downstream_task_b():
        return "Hi, I'm the downstream task B."

    @task(
        # since one of the branched tasks will always skip, we set the
        # trigger_rule of the downstream task to none_failed
        trigger_rule="none_failed"
    )
    def end():
        return "The end."

    chain(
        my_isolated_task(upstream_task()),
        [downstream_task_a(), downstream_task_b()],
        end(),
    )


branching_with_virtualenv_dag()
