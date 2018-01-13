from google.cloud.bigquery.client import Client
from google.cloud.bigquery import QueryJobConfig
from os import environ
import time
import logging


def bq_execute_query(google_client, query ,use_legacy_sql=False):
    #job_config = QueryJobConfig()
    #job_config.use_legacy_sql =use_legacy_sql
    query_job = google_client.query(query=query)
    retry_count = 100
    while retry_count > 0 and query_job.state != 'DONE':
        retry_count -= 1
        time.sleep(3)
        query_job.reload()  # API call
        logging.info("job state : %s " % (query_job.state)
                     )
    logging.info("job state : %s at %s" % (query_job.state, query_job.ended))

def bq_create_table_as_select(google_client,dataset_id,table_name,query):

    table_ref = google_client.dataset(dataset_id).table(table_name)
    job_config = QueryJobConfig()

    job_config.destination = table_ref
    job_config.write_disposition = "WRITE_TRUNCATE"
    query_job = google_client.query(
                             query=query,
                             job_config=job_config
                             )
    retry_count = 100
    while retry_count > 0 and query_job.state != 'DONE':
        retry_count -= 1
        time.sleep(3)
        query_job.reload()  # API call
        logging.info("job state : %s " % (query_job.state)
    )
    logging.info("job state : %s at %s" % (query_job.state, query_job.ended))

