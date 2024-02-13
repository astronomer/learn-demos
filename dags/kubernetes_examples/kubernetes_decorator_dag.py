"""
## Toy example of using the @task.kubernetes decorator

The @task.kubernetes decorator is used to run any Python code in an
isolated Kubernetes pod. It is the decorator version of the KubernetesPodOperator.

The DAG shows how to use the @task.kubernetes decorator and how to pass
[XCom](https://docs.astronomer.io/learn/airflow-passing-data-between-tasks) in an out of the task,
which is an action greatly simplified by using the decorator over the KubernetesPodOperator.

You will need to have a Kubernetes cluster running to use this DAG. You can either:
- Connect to any Kubernetes cluster by providing an Airflow connection to it. 
- If you are running Airflow on Kubernetes, you can use the in-cluster configuration. 

Learn more: 
- [Run tasks in an isolated environment in Apache Airflow](https://docs.astronomer.io/learn/airflow-isolated-environments) guide.
- [Use the KubernetesPodOperator](https://docs.astronomer.io/learn/kubepod-operator#use-the-taskkubernetes-decorator) guide.
- [KubernetesPodOperator full list of parameters](https://registry.astronomer.io/providers/apache-airflow-providers-cncf-kubernetes/versions/latest/modules/KubernetesPodOperator)
"""

from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd 
import sys
from airflow.configuration import conf

# get the current Kubernetes namespace Airflow is running in
namespace = conf.get("kubernetes", "NAMESPACE")

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    doc_md=__doc__,
    description="@task.kubernetes",
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["@task.kubernetes"],
)
def kubernetes_decorator_dag():

    @task
    def upstream_task():
        print(f"The python version in the upstream task is: {sys.version}")
        print(f"The pandas version in the upstream task is: {pd.__version__}")
        return 1

    @task.kubernetes(
        image="python",
        in_cluster=True,
        namespace=namespace,
        name="my_pod",
        get_logs=True,
        log_events_on_failure=True,
        do_xcom_push=True,
    )
    def t1(arg):
        return arg + 1

    @task
    def downstream_task(arg):
        print(f"The python version in the downstream task is: {sys.version}")
        print(f"The pandas version in the downstream task is: {pd.__version__}")
        return arg


    downstream_task(t1(upstream_task()))


kubernetes_decorator_dag()
