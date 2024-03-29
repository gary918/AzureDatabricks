import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from datetime import timedelta, datetime

# default arguments
args = {
    'owner': 'Airflow',
    'email': ['<your-email-address>'],
    'email_on_failure' : True,
    'depends_on_past': False,
    'databricks_conn_id': 'garyadbtest'
}

# DAG with Context Manager
# refer to https://airflow.apache.org/docs/stable/concepts.html?highlight=connection#context-manager
with DAG(dag_id='adb_pipeline', default_args=args, start_date=airflow.utils.dates.days_ago(1), schedule_interval = None) as dag:

	# job 1 definition and configurable through the Jobs UI in the Databricks workspace
	notebook_1_task = DatabricksRunNowOperator(
		task_id='notebook_1',
		job_id='605685723883450', 
		json= {
			"notebook_params": {
				'myname': 'Tom'
			}	
		})

	# Arguments can be passed to the job using `notebook_params`, `python_params` or `spark_submit_params`
	json_2 = {
		"notebook_params": {
			'myname': 'Gary'
		}
	}

	notebook_2_task = DatabricksRunNowOperator(
		task_id='notebook_2',
		job_id='807487835601929', 
		json=json_2)

	# input parameters for job 3
	json_3 = {
		"notebook_params": {
			'myname': 'John'
		}
	}

	notebook_3_task = DatabricksRunNowOperator(
		task_id='notebook_3',
		job_id='876286909181322', 
		json=json_3)

	# Define the order in which these jobs must run using lists
	[notebook_1_task, notebook_2_task] >> notebook_3_task