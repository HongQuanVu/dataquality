from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook,BigQueryCursor,BigQueryConnection
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_download_operator import *
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from datetime import date
from datetime import datetime
from datetime import timedelta
from sql_utils import get_table_name_and_selected_phase_from_create_as_sql,CREATE_TABLE_KEY_WORD
from os import environ
from google.cloud import bigquery
import time

SQL_SEPARATOR=";"
SCHEMA_NAME_TABLE_NAME_SEPARATOR ="."
STANDARD_SQL_NOTE="#standard sql"


def execute_asyn_insert_from_select(google_client, target_table_name, selected_sql):

    now         =   datetime.today()
    business_time = datetime.strftime(now, "%Y_%m_%d_%H_%M_%s")




    dataset_name  = target_table_name.split(SCHEMA_NAME_TABLE_NAME_SEPARATOR)[0]
    table_name  =   target_table_name.split(SCHEMA_NAME_TABLE_NAME_SEPARATOR)[1]

    job_name = "%s_%s" % (table_name , business_time)

    job = google_client.run_async_query(query=selected_sql, job_name=job_name)

    dataset     =   google_client.dataset(dataset_name)
    table       =   dataset.table(name=table_name)
    job.destination =table
    job.write_disposition ="WRITE_TRUNCATE"
    job.begin()
    retry_count = 100
    while retry_count > 0 and job.state != 'DONE':
        retry_count -= 1
        time.sleep(3)
    job.reload()  # API call
    logging.info("job state : %s at %s"%(job.state,job.ended))



#def execute_create_table()
def execute_multi_queries(google_client, source_sql):
    sql_statements = source_sql.split(SQL_SEPARATOR)
    for sql in sql_statements:
        logging.info(sql)
        #check if this is a create table statement
        if CREATE_TABLE_KEY_WORD in sql :
            table_name,selected_phase = get_table_name_and_selected_phase_from_create_as_sql(created_table_sql=sql)
            logging.info("Execute create table as command ...%s"%(sql))
            execute_asyn_insert_from_select(google_client=google_client,target_table_name=table_name,selected_sql=selected_phase)
            return
        if STANDARD_SQL_NOTE in sql:
            exec_sql = google_client.run_sync_query(sql)
            exec_sql.use_legacy_sql = False
            exec_sql.run()
        else:
            exec_sql = google_client.run_sync_query(sql)
            exec_sql.run()

def google_client_exec_multi_sqls(bigquery_credentials_file_path, source_sql, **kwargs):

    business_date = datetime.strftime(kwargs['execution_date'], "%Y-%m-%d")
    source_sql = source_sql.replace("$BUSINESS_DATE", "'%s'" % (business_date))
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = bigquery_credentials_file_path

    client = bigquery.Client()

    execute_multi_queries(google_client=client, source_sql=source_sql)


def execute_big_queries(bigquery_conn_id,multi_sqls,sql_separator=";",use_legacy_sql=False,**kwargs):

    hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id
                        )
    conn = hook.get_conn()
    logging.info("Execute : "+ multi_sqls)
    for sql in multi_sqls.split(sql_separator):

        cursor = conn.cursor()
        cursor.run_query(bql=sql, destination_dataset_table =None ,
                         allow_large_results =True,use_legacy_sql=False)
    logging.info("Execute : Done ")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.combine(date.today(), datetime.min.time()) -timedelta(1) ,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
 
# TODO: Replace with your project ID.
PROJECT_ID = 'acm-staging'
CONNECTION_ID = 'acm-staging'
BQ_DATASET_NAME = 'data_quality'
BQ_TABLE_NAME_PREFIX = 'measures_'
# TODO: Replace with your bucket ID.
GCS_BUCKET_ID = 'dataprep-acm-prod-dataquality'
bq_credential_file_path = "/Users/quan.vu/Downloads/acm-staging-etl-dataflow.json"
bq_credential_file_path ="/Users/quan.vu/Downloads/acm-staging-etl-dataflow.json"
RETRIES = 1
RETRY_DELAY=1

dt = datetime.today()

 
with DAG('gcp_dag', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
 
    # Format table name template.
    table_name_template = PROJECT_ID + '.' + BQ_DATASET_NAME + '.' + BQ_TABLE_NAME_PREFIX + '{{ ds_nodash }}'
 
    # Format GCS export URI template.
    gcs_export_uri_template = 'gs://' + GCS_BUCKET_ID + '/daily_exports/{{ ds_nodash }}/part-*.gz'
 
    # 1) Compute data.
   # bq_compute_yesterday_data = BigQueryOperator(
   #     task_id = 'bq_compute_yesterday_data',
   #     bql = 'gcp_dag/query_template.sql',
   #     destination_dataset_table = table_name_template,
   #     write_disposition = 'WRITE_TRUNCATE',
   #     bigquery_conn_id = CONNECTION_ID,
   #     use_legacy_sql = False
   # )

    # 2)
    sql_command = """
                        #standard sql
                        CREATE TABLE data_quality.tmp_test_date2
                        AS 
                        SELECT * FROM data_quality.TMP_TEST_DATE
                        ;
                        #standard sql
                        INSERT INTO data_quality.TMP_TEST_DATE(BUSINESS_DATE)
                        SELECT
                        CURRENT_DATE 
                        ;
                        #standard sql
                        SELECT 1
                    """
    #run_multi_queries_1  =PythonOperator(task_id="Run_Multiple_queries_1",python_callable=google_client_exec_multi_sqls,
    #                                   op_kwargs = {'bigquery_conn_id': CONNECTION_ID,
    #                                                 'multi_sqls' :sql_command
    #                                                }
    #                                  ,
    #                                   )


    #run_multi_queries_2  =PythonOperator(task_id="Run_Multiple_queries_2",python_callable=google_client_exec_multi_sqls,
    #                                   op_kwargs = {'bigquery_conn_id': CONNECTION_ID,
    #                                                 'multi_sqls' :sql_command
    #                                               }
    #                                   ,
    #                                   )
    #run_multi_queries_2.set_upstream(run_multi_queries_1)

    run_multi_queries_3 = PythonOperator(
       dag=dag, task_id="load_bg_current_date_1",
        provide_context=True,
        python_callable=google_client_exec_multi_sqls,
        retries=RETRIES,
        retry_delay=RETRY_DELAY,
        op_kwargs={
            'bigquery_credentials_file_path': bq_credential_file_path,
            'source_sql': sql_command
        }
    )

    run_multi_queries_4 = PythonOperator(
       dag=dag, task_id="load_bg_current_date_2",
        provide_context=True,
        python_callable=google_client_exec_multi_sqls,
        retries=RETRIES,
        retry_delay=RETRY_DELAY,
        op_kwargs={
            'bigquery_credentials_file_path': bq_credential_file_path,
            'source_sql': sql_command
        }
    )
    run_multi_queries_4.set_upstream(run_multi_queries_3)