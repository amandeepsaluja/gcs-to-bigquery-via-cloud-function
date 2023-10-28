import functions_framework
import pandas_gbq
import yaml

import pandas as pd

from datetime import datetime
from google.cloud import storage
from helpers import clean_column_name
from io import BytesIO


@functions_framework.cloud_event
def process_gcs_excel(cloudevent):
    # Getting payload data from the Cloud Storage event
    payload = cloudevent.data.get("protoPayload")
    resource_name = payload.get("resourceName")

    # parsing configuration
    with open("config.yaml", "r") as yaml_file:
        config_data = yaml.load(yaml_file, Loader=yaml.FullLoader)

    # Extract the GCS bucket and object details
    gcs_file_location = resource_name.split("/", maxsplit=3)[-1].replace(
        "/objects/", "/"
    )
    gcs_bucket_name = gcs_file_location.split("/", maxsplit=1)[0]
    gcs_file_path = gcs_file_location.split("/", maxsplit=1)[-1]

    print(f"File uploaded: {gcs_file_path}")

    # instantiate storage client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_file_path)
    result = blob.download_as_bytes()

    # read excel file
    df = pd.read_excel(BytesIO(result), sheet_name=0)

    # updating column names and converting to a dictionary
    df.columns = [clean_column_name(col) for col in df.columns]

    # adding metadata columns
    df["pipeline_source"] = "Cloud Function"
    df["file_path"] = f"gs://{gcs_file_location}"
    # Add a DATETIME column with the current timestamp
    current_time = datetime.now()
    df["load_datetime"] = current_time
    df["load_date"] = current_time.date()

    # writing to BigQuery
    pandas_gbq.to_gbq(
        df,
        f"{config_data['BQ_DATASET']}.{config_data['BQ_TABLE']}",
        project_id=config_data["PROJECT_ID"],
        if_exists="append",
    )

    return print(f"{gcs_file_path} file processed successfully!")
