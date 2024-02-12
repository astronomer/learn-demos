"""
## Toy example of using the IsolatedOperator

The IsolatedOperator is used to run any existing operator in an
isolated Kubernetes pod. 

The DAG shows how to use the IsolatedOperator and how to pass
[XCom](https://docs.astronomer.io/learn/airflow-passing-data-between-tasks) in an out of the task.

You will need to have a Kubernetes cluster running to use this DAG. You can either:
- Connect to any Kubernetes cluster by providing an Airflow connection to it. 
- If you are running Airflow on Kubernetes, you can use the in-cluster configuration. 

Learn more: 
- [Run tasks in an isolated environment in Apache Airflow](https://docs.astronomer.io/learn/airflow-isolated-environments) guide.
- [Isolation Provider README](https://github.com/astronomer/apache-airflow-providers-isolation/blob/main/README.md).
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
import pandas as pd
import sys  
import os

@dag(
    start_date=None,
    schedule=None,
    doc_md=__doc__,
    description="IsolatedOperator",
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["IsolatedOperator"],
)
def isolated_operator_dag():

    @task
    def upstream_task():
        print(f"The python version in the upstream task is: {sys.version}")
        print(f"The pandas version in the upstream task is: {pd.__version__}")
        return {"num": 1, "word": "hello"}

    @task
    def t1(arg):
        return arg + 1

    @task
    def downstream_task(arg):
        print(f"The python version in the downstream task is: {sys.version}")
        print(f"The pandas version in the downstream task is: {pd.__version__}")
        return arg


    downstream_task(t1(upstream_task()))


isolated_operator_dag()
