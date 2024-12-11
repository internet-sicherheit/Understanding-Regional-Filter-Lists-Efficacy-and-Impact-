import pandas as pd
import json
import sqlite3
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud.bigquery import LoadJobConfig, SchemaUpdateOption
import os
from glob import glob

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "resources", "google.json")


# Function to load a chunk to BigQuery
def load_chunk_to_bigquery(chunk_df, table_id, client, job_config):
    try:
        job = client.load_table_from_dataframe(
            chunk_df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Chunk loaded successfully to {table_id}")
    except Exception as e:
        print(f"Failed to load chunk to {table_id} with error: {e}")


def read_from_json():
    path = ""
    folder = glob(path+"*.json")

    data = list()
    for file in folder:
        print("Read from file", file)
        with open(file, 'r', encoding="utf-8") as f:
            data.append(json.loads(f.read()))

    print("Read", len(data), "files")
    return data


# Initialize a BigQuery client
client = bigquery.Client()

# BigQuery dataset name
dataset_name = 'measurement'

# Tables to transfer from SQLite to BigQuery
tables = ['tmp_url_tracker']

for table in tables:
    job_config = LoadJobConfig(schema_update_options=[
                               SchemaUpdateOption.ALLOW_FIELD_ADDITION])
    table_id = f"{client.project}.{dataset_name}.{table}"

    d = read_from_json()
    for chunk in d:
        # Ensure column data types are correct
        #for col in chunk.columns:
        #    if col in ['site_id']:  # Specify other columns as needed
        #        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
        #    elif col == 'init_stack':
        #        # Assuming 'init_stack' should be a string, handle any special cases:
        #        chunk[col] = chunk[col].astype(str)

        for attempt in range(4):
            try:
                load_chunk_to_bigquery(pd.DataFrame(chunk), table_id, client, job_config)
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt == 3:
                    print(
                        f"Failed to load chunk to {table_id} after 4 attempts")

