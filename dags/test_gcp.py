from google.cloud import bigquery
from os import environ
from datetime import *
bq_credential_file_path ="/Users/quan.vu/Downloads/acm-staging-etl-dataflow.json"

environ["GOOGLE_APPLICATION_CREDENTIALS"] = bq_credential_file_path

client = bigquery.Client()

sql_command  ="SELECT * FROM data_quality.TMP_TEST_DATE"

now =datetime.today()


business_time = datetime.strftime(now, "%Y-%m-%d_%H_%M_%s")

client = bigquery.Client()
query = sql_command

dataset = client.dataset('data_quality')
tmp_name= 'tmp_load'
table = dataset.table(name='tmp_load')
job_name = "%s_%s"%(tmp_name,business_time)
job = client.run_async_query(job_name =job_name, query=query)
job.name ="%s_%s"%(tmp_name,business_time)
job.destination = table
job.write_disposition= 'WRITE_APPEND'

job.begin()

print (job.state)

import time
retry_count = 100
while retry_count > 0 and job.state != 'DONE':
     retry_count -= 1
     time.sleep(10)
     job.reload()  # API call
     print (job.state)

print (job.state)
print ( job.ended)
