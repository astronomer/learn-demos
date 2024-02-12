# syntax=quay.io/astronomer/airflow-extensions:v1

FROM quay.io/astronomer/astro-runtime:10.3.0-python-3.11

# create a virtual environment for the PythonVirtualenvOperator and @task.virtualenv decorator
# using Python 3.10. The operator/decorator will only reference the Python version bin directly
PYENV 3.10 pyenv_3_10

# create a virtual environment for the ExternalPythonOperator and @task.external_python decorator
# using Python 3.9 and install the packages from epo_requirements.txt
PYENV 3.9 epo_pyenv epo_requirements.txt
