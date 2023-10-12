from salesforce_bulk import CsvDictsAdapter, SalesforceBulk
import csv
from salesforce_bulk.util import IteratorBytesIO
from io import StringIO
import pandas as pd

# Function to split a list into batches of a given size
def batch_list(input_list, batch_size):
    for i in range(0, len(input_list), batch_size):
        yield input_list[i:i + batch_size]

CSV_path =  "F:\\Work_files\\Do not touch - issue_09_15 - Sheet1 (2).csv"

df = pd.read_csv(CSV_path)
df.fillna('', inplace=True)
#df.drop(columns=['Address'], inplace=True)
update_list = df.to_dict('records')

# set up the bulk API connection
bulk = SalesforceBulk(username='akshar.dharwadkar@webware.io',
                      password='Akshar@1995',
                      security_token='nAIj2ETyLBP6zouVa9bQqqd06',
                      sandbox=False)

# Authenticate with Salesforce and create a bulk job
job = bulk.create_update_job(object_name='Lead', contentType='CSV')

batch_size = 10000  # Size of each batch
batches = batch_list(update_list, batch_size)

# Initialize empty dataframes to store results
success_df = pd.DataFrame()
error_df = pd.DataFrame()
batch_df = pd.DataFrame()

for batch_data in batches:
    # Create CSV adapter for the current batch and upload CSV data to Salesforce for updating
    csv_adapter = CsvDictsAdapter(iter(batch_data))
    batch = bulk.post_batch(job, csv_adapter)

    # Wait for batch completion
    bulk.wait_for_batch(job, batch, 60 * 10)

    # Get batch results and update success and error dataframes
    batch_results = bulk.get_batch_results(batch_id=batch, job_id=job)
    df2 = pd.DataFrame(batch_results)
    success_batch = df2.loc[df2['success'] == "true"]
    error_batch = df2.loc[df2['success'] == "false"]

    success_df = pd.concat([success_df, success_batch])
    error_df = pd.concat([error_df, error_batch])
    batch_df = pd.concat([batch_df, df2])

# Save the combined results to CSV files
success_df.to_csv("Results\\Success.csv", index=False)
error_df.to_csv("Results\\Error.csv", index=False)
batch_df.to_csv("D:\\Salesforce_auto\\output\\opp_update.csv", index=False)

# Close the bulk job after all batches are processed
bulk.close_job(job)
