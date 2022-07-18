import os
import logging
import json
import ndjson
import yaml
import uuid
import re
from datetime import datetime

from functools import wraps
from flask import Flask, request, jsonify

import google.cloud.storage
import google.cloud.bigquery
import google.api_core.exceptions
from google.cloud import pubsub_v1
from google.cloud.bigquery.schema import SchemaField
from google.api_core import retry

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logging.getLogger("google.resumable_media._helpers").setLevel(logging.WARNING)
logging.basicConfig(
    format="[%(asctime)s] [%(name)s] [%(funcName)s] [%(levelname)s] %(message)s",
    level=LOG_LEVEL,
)
logger = logging.getLogger(__name__)

storage_client = google.cloud.storage.Client()
bigquery_client = google.cloud.bigquery.Client()
publisher = pubsub_v1.PublisherClient()

def gcs2bq():
    bucket_config_name = "pmr-gcs-test-ew9"
    bucket_source_name = "pmr-gcs-test-ew9"
    bucket_archive = ""
    bucket_error = ""
    source_input_file = "test/raw_twitter_tweet_metrics_v1.json"
    target_dataset = "pmr_bqd_test_ew1"
    target_table = "raw_load_json_tbl"
    schema_file = "test/raw_twitter_tweet_metrics_v1_schema.json"
    bq_wrtdspt = "WRITE_TRUNCATE"
    
    logger.debug(
        f"> Variables {bucket_config_name} |  {bucket_source_name} | {source_input_file} | {target_dataset} | {target_table} | {schema_file} "
    )
    
    #bucket = storage_client.get_bucket(bucket_config_name)
    #blob = bucket.blob(schema_file)
    #json_schema_read = json.loads(blob.download_as_string(client=None))
    
    with open(schema_file) as fp:
        json_schema_read = json.load(fp)
    
    with open(source_input_file) as fs:
        source_uri = ndjson.load(fs)
    #source_uri = f"gs://{bucket_source_name}/{source_input_file}"
    
    destination_table = f"{target_dataset}.{target_table}"
    
    print(destination_table)
    
    json_job_config = google.cloud.bigquery.LoadJobConfig(
        schema=json_schema_read,
        write_disposition= google.cloud.bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=google.cloud.bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    # Query job to ingest data
    json_load_job = bigquery_client.load_table_from_file(
        source_uri,
        destination_table,
        location='europe-west1',  # Location must match that of the destination dataset.
        #retry=retry.Retry(deadline=30),
        # write_disposition=bq_writedisposition,
        job_config=json_job_config
    )
    
    try:
        json_load_job.result()  # Waits for the job to complete.
        json_destination_table = bigquery_client.get_table(destination_table)
        print("Loaded {} rows.".format(json_destination_table.num_rows))
    except google.api_core.exceptions.BadRequest as e:
        #TODO Agregar mas detalle sobre el error, utilizar otras librerias para complementar el mensaje de error, como por ejemplo, lo que se encuentra en el protoPayload de Cloud Logging.
        return handle_error(400, "System error", f"{e.errors[0].get('message')}")
    
    records_written = format(json_destination_table.num_rows)
    logger.info(f"Succesfully inserted {records_written} ")

if __name__ == '__main__':
    gcs2bq()