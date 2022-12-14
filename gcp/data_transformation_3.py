import logging
import argparse
import time
import re
from datetime import datetime as dt
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
import apache_beam as beam


def replace_nulls(element):
    """This function takes a string with comma separated values as input and
    replaces all NULL values with an and empty string

    Example input string:
    '10000085,1526,7239,10/03/2020 13:28,Bloor St W / Manning Ave - SMART,NULL,
    10/03/2020 13:53,Foster Pl / Elizabeth St - SMART,3956,Annual Member'

    Example output string:
    '10000083,720,7239,2020-10-03 13:28:00,Bloor St W / Manning Ave - SMART,,
    2020-10-03 13:40:00,King St W / Tecumseth St,5563,Annual Member'

    """
    return element.replace('NULL','')

def parse_method(string_input):
    """This method translates a single line of comma separated values to a dictionary 
    which can be loaded into BigQuery.

    Args:
    string_input: A comma separated list of values in the form: 'Trip_Id, Trip__Duration,
    Start_Station_Id, Start_Time, Start_Station_Name, End_Station_Id, End_Time, 
    End_Station_Name, Bike_Id, User_Type'

    Example string_input: '10000083,720,7239,2020-03-10 13:28:00,
    Bloor St W / Manning Ave - SMART,7160, 10/03/2020 13:40,
    King St W / Tecumseth St,5563,Annual Member'

    Returns:
    A dict mapping BigQuery column names as keys to the corresponding value
    parsed from string_input.

    Example output:
        {'Trip_Id':'10000083',
        'Trip__Duration':'720',
        'Start_Station_Id':'7239',
        'Start_Time':'2020-03-10 13:28:00',
        'Start_Station_Name':'Bloor St W / Manning Ave - SMART',
        'End_Station_Id':'7160',
        'End_Time':'2020-10-03 13:40:00',
        'End_Station_Name':'King St W / Tecumseth St',
        'Bike_Id':'5563',
        'User_Type':'Annual Member'}
    """
    # Strip out carriage return, newline and quote characters.
    values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
    row = dict(zip(('year_post','name_day_of_week','n_reg'),values))
    return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from csv into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to read and the output table to write.
    # This is the final stage of the pipeline, where we define the destination
    # of the data. In this case we are writing to BigQuery.
    parser.add_argument('--input',  dest='input',   required=False, help='Input file to read. This can be a local file or a file in a Google Storage Bucket.')

    # This defaults to the bucket in your BigQuery project. You'll have
    # to create the bucket yourself using this command:
    # bq mk my-bucket
    parser.add_argument('--output', dest='output',  required=False, help='Output BQ table to write results to.')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-3-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    # Table schema for BigQuery
    table_schema = 'year_post:INTEGER,name_day_of_week:STRING,n_reg:BIGNUMERIC'

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=options)

    (p
    # Read the file. This is the source of the pipeline. All further
    # processing starts with lines read from the file. We use the input
    # argument from the command line. We also skip the first line which is a
    # header row.
    | 'Read_from_GCS' >> beam.io.ReadFromText(opts.input, skip_header_lines=1)

    # This stage of the pipeline reads individual rows and transforms them
    # according to the logic defined in the functions
    #| 'Deconcactenate Columns' >> beam.Map(deconcat)
    | 'Replace_Nulls' >> beam.Map(replace_nulls)
    #| 'Convert to BQ Datetime' >> beam.Map(format_datetime_bq)

    # This stage of the pipeline translates from a CSV file single row
    # input as a string, to a dictionary object consumable by BigQuery.
    # It refers to a function we have written. This function will
    # be run in parallel on different workers using input from the
    # previous stage of the pipeline.
    | 'String To BigQuery Row' >> beam.Map(parse_method)
    | 'Write_to_BigQuery' >> beam.io.WriteToBigQuery(
    # The table name is a required argument for the BigQuery sink.
    # In this case we use the value passed in from the command line.
    opts.output,
    # Here we use the simplest way of defining a schema:
    # fieldName:fieldType
    schema=table_schema,
    # Creates the table in BigQuery if it does not yet exist.
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    # Deletes all data in the BigQuery table before writing.
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    p.run().wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()