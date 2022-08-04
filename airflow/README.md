# Azure Databricks and Apache Airflow Integration
## Setup Azure Databricks Environment
* Create a new Databricks cluster
* Create the following jobs and record their job ids
  * job1 by using notebook1
  * job2 by using notebook2
  * job3 by using notebook3
## Install Apache Airflow
* Open a terminal, execute ./airflow_install_run.sh
* Run ```airflow webserver --port 8080```
* Open a new terminal, run ```airflow scheduler```
* Open a browser and access http://localhost:8080
## Create Workflow and Run
* Copy the /dags folder under ~/airflow, you'll be able to see the 'adb_pipeline' in the DAG list
* Update the job_id values
* In the connection list, edit 'databricks_default' connection with correct Azure Databricks URL and access token.
* Trigger 'adb_pipeline' and observe its status
