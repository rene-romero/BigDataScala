import logging
import argparse
import time
import re
from google.cloud import storage
from datetime import datetime as dt
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
import apache_beam as beam

def keys_from_schema_txt(b, p):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(b)
    blob = bucket.blob(p)
    keys_1 = blob.download_as_text()
    keys_2 = list(item.split(":") for item in keys_1.split("\n"))
    keys_3 = dict(keys_2)
    keys = tuple(keys_3.keys())
    return keys

def schema_txt(b, p):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(b)
    blob = bucket.blob(p)
    table_schema_1 = blob.download_as_text()
    table_schema_2 = list(item.split(":") for item in table_schema_1.split("\n"))
    table_schema_3 = dict(table_schema_2)
    schema = str()
    for key in table_schema_3:
        schema += key + ":" + table_schema_3[key] + ","
    table_schema = schema.strip(",")
    return table_schema

def replace_nulls(element):
    return element.replace('NULL','')

def parse_method(string_input, keys=None):
    values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
    row = dict(zip(keys,values))
    return row

def run(**kwargs):
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = kwargs.get('project')
    options.view_as(GoogleCloudOptions).region = kwargs.get('region')
    options.view_as(GoogleCloudOptions).staging_location = kwargs.get('stagingLocation')
    options.view_as(GoogleCloudOptions).temp_location = kwargs.get('tempLocation')
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-test-',time.time_ns())
    options.view_as(StandardOptions).runner = kwargs.get('runner')

    uri = kwargs.get('schema')
    matches = re.match("gs://(.*?)/(.*)", uri)
    if matches:
        bucket_name, path_name = matches.groups()
    bucket = bucket_name
    path = path_name

    keys_schema = keys_from_schema_txt(bucket, path)
    table_schema = schema_txt(bucket, path)

    p = beam.Pipeline(options=options)

    (p
    | 'Read_from_GCS' >> beam.io.ReadFromText(kwargs.get('input'), skip_header_lines=1)
    | 'Replace_Nulls' >> beam.Map(replace_nulls)
    | 'String To BigQuery Row' >> beam.Map(parse_method, keys = keys_schema)
    | 'Write_to_BigQuery' >> beam.io.WriteToBigQuery(
    kwargs.get('output'),
    schema=table_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    p.run().wait_until_finish()


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Load from csv into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input',  dest='input',   required=True, help='Input file to read. This can be a local file or a file in a Google Storage Bucket.')
    parser.add_argument('--output', dest='output',  required=True, help='Output BQ table to write results to.')
    parser.add_argument('--schema', dest='schema',  required=True, help='Input schema to apply in our data.')
    opts = parser.parse_args()
    
    logging.getLogger().setLevel(logging.WARNING)
    run(**vars(opts))