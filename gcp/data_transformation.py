import argparse
import time
import logging
import csv
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from CSV into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input', required=True, help='Specify the source of the file')
    parser.add_argument('--output', required=True, help='Specify the Dataset and table')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    # Static input and output
    input = opts.input
    output = opts.output

    def parse_method(string_input):
    # This method translates a single line of comma separated values to a dictionary 
    # which can be loaded into BigQuery.
    # Args:
    # string_input: A comma separated list of values in the form: 'Trip_Id, Trip__Duration,
    # Start_Station_Id, Start_Time, Start_Station_Name, End_Station_Id, End_Time, 
    # End_Station_Name, Bike_Id, User_Type'
    # Example string_input: '10000083,720,7239,2020-03-10 13:28:00,
    # Bloor St W / Manning Ave - SMART,7160, 10/03/2020 13:40,
    # King St W / Tecumseth St,5563,Annual Member'
    # Returns:
    # A dict mapping BigQuery column names as keys to the corresponding value
    # parsed from string_input.
    # Example output:
    #     {'Trip_Id':'10000083',
    #     'Trip__Duration':'720',
    #     'Start_Station_Id':'7239',
    #     'Start_Time':'2020-03-10 13:28:00',
    #     'Start_Station_Name':'Bloor St W / Manning Ave - SMART',
    #     'End_Station_Id':'7160',
    #     'End_Time':'2020-10-03 13:40:00',
    #     'End_Station_Name':'King St W / Tecumseth St',
    #     'Bike_Id':'5563',
    #     'User_Type':'Annual Member'}
    
    # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
        row = dict(
            zip(('year','followers','is_business_account','post_type','number_characters','number_hashtags','total_likes','total_comments'),values))
        return row

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "year",
                "type": "INTEGER"
            },
            {
                "name": "followers",
                "type": "STRING"
            },
            {
                "name": "is_business_account",
                "type": "BOOL"
            },
            {
                "name": "post_type",
                "type": "INTEGER"
            },
            {
                "name": "number_characters",
                "type": "STRING"
            },
            {
                "name": "number_hashtags",
                "type": "STRING"
            },
            {
                "name": "total_likes",
                "type": "INTEGER"
            },
            {
                "name": "total_comments",
                "type": "INTEGER"
            }
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)

    '''

    Steps:
    1) Read something
    2) Transform something
    3) Write something

    '''

    (p
        | 'Read_from_GCS' >> beam.io.ReadFromText(input, skip_header_lines=1)
        | 'Data_Transformation' >> beam.Map(parse_method)
        | 'Write_to_BigQuery' >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()