## Learn Demo for: Run tasks in an isolated environment in Apache Airflow

This repository contains example DAGs to learn about how to run Airflow tasks in isolated environments.
It is based on the Airflow concept guide [Run tasks in an isolated environment in Apache Airflow](https://docs.astronomer.io/learn/airflow-isolated-environments#use-airflow-context-variables-in-isolated-environments).

## Steps to run this repository

Download the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) to run Airflow locally in Docker. `astro` is the only package you will need to install.

1. Run `git clone --branch airflow-isolated-environments https://github.com/astronomer/learn-demos.git` on your computer to create a local clone of this repository branch.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.

Additional steps for specific DAGs:

- To run the `kubernetes_decorator_dag` and the `kubernetes_pod_operator_dag` you will need to have Airflow running on a Kubernetes cluster or connect the decorator/operator to a remote cluster. See [Use the KubernetesPodOperator](https://docs.astronomer.io/learn/kubepod-operator).
- To run the `use_secret` DAG you will need to define an Airflow variable called `my_secret` with any value. See [Use Airflow variables](https://docs.astronomer.io/learn/airflow-variables).
- To run the experimental `hook_in_pyenv` DAG you will need to have a Snowflake connection defined. See [Manage connections in Apache Airflow](https://docs.astronomer.io/learn/connections).
