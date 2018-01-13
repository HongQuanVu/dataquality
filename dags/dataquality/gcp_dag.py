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
from os import environ
from google.cloud import bigquery
import time
import psycopg2
from airflow.dags.dataquality.gcp_utils import *
from airflow.dags.dataquality.sql_utils import *

SQL_SEPARATOR=";"
SCHEMA_NAME_TABLE_NAME_SEPARATOR ="."
STANDARD_SQL_NOTE="#standard sql"


def execute_asyn_insert_from_select(google_client, target_table_name, selected_sql):

    now         =   datetime.today()
    business_time = datetime.strftime(now, "%Y_%m_%d_%H_%M_%s")

    dataset_name  = target_table_name.split(SCHEMA_NAME_TABLE_NAME_SEPARATOR)[0]
    table_name  =   target_table_name.split(SCHEMA_NAME_TABLE_NAME_SEPARATOR)[1]
    logging.info("dataset name =%s"%(dataset_name))
    logging.info("table name  =%s" %(table_name))
    logging.info("selected_sql = %s" % (selected_sql))
    bq_create_table_as_select(google_client=google_client,dataset_id=dataset_name,table_name=table_name,query=selected_sql)



#def execute_create_table()
def execute_multi_queries(google_client, source_sql):
    sql_statements = source_sql.split(SQL_SEPARATOR)
    for sql in sql_statements:
        logging.info(sql)
        #check if this is a create table statement
        if CREATE_TABLE_KEY_WORD in sql :
            full_table_name,selected_phase = get_table_name_and_selected_phase_from_create_as_sql(created_table_sql=sql)

            logging.info("Execute create table as command ...%s"%(sql))

            execute_asyn_insert_from_select(google_client=google_client,target_table_name=full_table_name,selected_sql=selected_phase)
        else :
            #if STANDARD_SQL_NOTE in sql:
            bq_execute_query(google_client=google_client,query=sql)
        #else:
        #bq_execute_query(google_client=google_client, query=sql, use_legacy_sql=True)

def google_client_exec_multi_sqls(bigquery_credentials_file_path, source_sql, **kwargs):

    business_date = datetime.strftime(kwargs['execution_date'], "%Y-%m-%d")
    yyymmdd   = datetime.strftime(kwargs['execution_date'], "%Y%m%d")
    source_sql = source_sql.replace("$BUSINESS_DATE", "%s" % (business_date))
    source_sql = source_sql.replace("$YYYYMMDD", "%s" % (yyymmdd))
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
bq_credential_file_path ="/Users/quan.vu/Downloads/ascend_money_data_platform_tester.json"
RETRIES = 1
RETRY_DELAY=1

dt = datetime.today()

midnight = datetime.combine(dt.date(), datetime.min.time())

start_business_date = midnight + \
    timedelta(hours=6, minutes=0) - timedelta(days=1)

schedule_interval = timedelta(days=1)

timeout_interval = timedelta(minutes=60 * 3)

# If unable to send email within an hour , notify

email_sla = timedelta(hours=1)

execution_date = """'{{ ds }}'"""  # '2017-02-01'
dv_yyyy_mm_dd = "{{ ds }}"   # 2017-02-01
dv_yyyymmdd = "{{ ds_nodash }}"  # 20170201


RETRIES = 1
RETRY_DELAY = timedelta(seconds=1)
SLA = timedelta(minutes=30)
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_business_date,
    'email': ['quanvu@chotot.vn'],
    'email_on_failure': True,
    'email_on_retry': True,
    'sla': timedelta(minutes=30),
    'retries': 60 * 10,
    'retry_delay': timedelta(minutes=2)
}


dt = datetime.today()
source_postgres_conn_string ="dbname='airflow' user='postgres' host='localhost' password='postgres'"
conn = psycopg2.connect(source_postgres_conn_string)
cur = conn.cursor()
cur.execute("""
    SELECT rule_id, 
           rule_name, 
           rule_short_name,
           entities, 
           rule_statement, 
           rule_quality_dimension, 
           rule_expression, 
           rule_execution_engine
    FROM dataquality.data_quality_rule
""")
rows =cur.fetchall()
for row in rows :
    rule_id =row[0]
    rule_name = row[1]
    rule_short_name= row[2]
    rule_expression =row[6]
    print(rule_expression)
    logging.info(rule_expression)
    dag_id = 'DQ_RULE_%s_%s' % (rule_id,rule_short_name)

    dag = DAG(
        dag_id=dag_id,
        default_args=args,
        schedule_interval=schedule_interval,
        dagrun_timeout=timeout_interval,
        start_date=start_business_date
    )

    globals()[dag_id] = dag
    sql_command = rule_expression
    PythonOperator(
             dag=dag, task_id="DQ_RUN_RULE_CHECK",
             provide_context=True,
             python_callable=google_client_exec_multi_sqls,
             retries=RETRIES,
             retry_delay=RETRY_DELAY,
             op_kwargs={
                 'bigquery_credentials_file_path': bq_credential_file_path,
                 'source_sql': sql_command
             }
         )

if __name__ == "__main__":
    print ("load DAG..")

