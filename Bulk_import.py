from salesforce_bulk import CsvDictsAdapter, SalesforceBulk
import csv
from salesforce_bulk.util import IteratorBytesIO
from io import StringIO
import pandas as pd


#CSV_path = "csv_templates\\Insert_data.csv"
CSV_path = "F:\\Work_files\\Upload_08_25 - Sheet4 (1).csv"

df = pd.read_csv(CSV_path)
df.fillna('', inplace=True)
#df.drop(columns=['Address'],inplace=True)
upload_list = df.to_dict('records')


# set up the bulk API connection
bulk = SalesforceBulk(username='md.rabbani@webware.io.lead',
                      password='Webware@1234',
                      security_token='Et5QWCA6gBR5TV99RKsWCYlv',
                      sandbox=True)


# Authenticate with Salesforce and create a bulk job
job = bulk.create_insert_job(object_name='Task', contentType='CSV')

# Create CSV adapter and upload CSV data to Salesforce
csv_adapter = CsvDictsAdapter(iter(upload_list))
batch = bulk.post_batch(job, csv_adapter)

# Close the bulk job and get the results
bulk.close_job(job)
bulk.get_query_batch_request(batch,job)
bulk.wait_for_batch(job,batch,60*10)
batch_results = bulk.get_batch_results(batch_id=batch, job_id=job)

df2 = pd.DataFrame(batch_results)
result = pd.concat([df, df2], axis=1)
sucess_df = result.loc[result['success'] == "true"]
Error_df = result.loc[result['success'] == "fales"]
sucess_df.to_csv("Results\Sucess.csv", index=False)
Error_df.to_csv("Results\Error.csv", index=False)
