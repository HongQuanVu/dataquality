from google.cloud import bigquery
from os import environ
from datetime import *
from airflow.dags.dataquality.gcp_utils import  *
bq_credential_file_path ="/Users/quan.vu/Downloads/acm-staging-etl-dataflow.json"

environ["GOOGLE_APPLICATION_CREDENTIALS"] = bq_credential_file_path

client = bigquery.Client()

sql_command  ="SELECT * FROM data_quality.TMP_TEST_DATE"

now =datetime.today()
