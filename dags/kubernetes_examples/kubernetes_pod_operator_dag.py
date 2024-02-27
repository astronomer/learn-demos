"""
## Toy example of using the KubernetesPodOperator

The KubernetesPodOperator is used to run any Python code in an
isolated Kubernetes pod. It is the traditional operator version of the @task.kubernetes decorator.

The DAG shows how to use the KubernetesPodOperator.
To learn how to use the KPO with XCom see the 
[KubernetesPodOperator guide](https://docs.astronomer.io/learn/kubepod-operator#example-use-the-kubernetespodoperator-with-xcoms).

You will need to have a Kubernetes cluster running to use this DAG. You can either:
- Connect to any Kubernetes cluster by providing an Airflow connection to it. 
- If you are running Airflow on Kubernetes, you can use the in-cluster configuration. 

Learn more: 
- [Run tasks in an isolated environment in Apache Airflow](https://docs.astronomer.io/learn/airflow-isolated-environments) guide.
- [Use the KubernetesPodOperator](https://docs.astronomer.io/learn/kubepod-operator#use-the-taskkubernetes-decorator) guide.
- [KubernetesPodOperator full list of parameters](https://registry.astronomer.io/providers/apache-airflow-providers-cncf-kubernetes/versions/latest/modules/KubernetesPodOperator)
"""

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from pendulum import datetime
from airflow.configuration import conf

# get the current Kubernetes namespace Airflow is running in
namespace = conf.get("kubernetes_executor", "NAMESPACE")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    doc_md=__doc__,
    description="KubernetesPodOperator",
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["KubernetesPodOperator"],
)
def kubernetes_pod_operator_dag():

    KubernetesPodOperator(
        task_id="my_isolated_task",
        namespace=namespace,
        image="hello-world",
        name="my-pod",
        in_cluster=True,
        is_delete_operator_pod=True,
        get_logs=True,
    )


kubernetes_pod_operator_dag()
