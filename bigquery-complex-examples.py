#!/usr/bin/env python

from google.cloud import bigquery
from google.api_core import exceptions
import argparse
import gzip
import random
import string
import json
import uuid
import time
from datetime import datetime, timedelta

def validate_credentials():
    """
Check and see if we have a valid credentials file,
just to save folks some heartache.
    """
    try:
        client = bigquery.Client()
        client.list_projects()
    except EnvironmentError:
        exit("""
    Error: Unable to access BigQuery, did you set the
    GOOGLE_APPLICATION_CREDENTIALS
    environment variable to the path to your JSON file, like so:

    export GOOGLE_APPLICATION_CREDENTIALS="~/MyProject-1234.json"
            """)

def create_dataset(name, description):
    """
Creates a new BigQuery dataset with the selected name.
    """
    client = bigquery.Client()

    dataset_ref = client.dataset(name)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.description = description
    try:
        dataset = client.create_dataset(dataset)
        print("Done, %s created." % name)
    except google.api_core.exceptions.Conflict:
        print("Error: %s already exists." % name)

def create_table(dataset, name, description):
    """
Creates a new BigQuery table inside the dataset with the selected name.
    """

    SCHEMA = [
        bigquery.SchemaField('visit_id', 'INT64',
            mode='required', description="Visit ID"),
        bigquery.SchemaField('visit_time', 'TIMESTAMP',
            mode='required', description="Visit Time"),
        bigquery.SchemaField('payload', 'STRUCT', mode='REQUIRED', fields = [
            bigquery.SchemaField('visit_location', 'STRING',
                mode='required', description="Visit Location"),
            bigquery.SchemaField('metadata', 'STRUCT', mode='REPEATED', fields = [
                bigquery.SchemaField('key', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('value', 'STRING')
            ]),
            bigquery.SchemaField('metrics', 'STRUCT', mode='REPEATED', fields = [
                bigquery.SchemaField('key', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('value', 'FLOAT64')
            ])
        ])
    ]
    table_ref = dataset.table(name)
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table.description = description
    #table.partitioning_type = 'DAY'
    try:
        client = bigquery.Client()
        created_table = client.create_table(table)
        print("Done, %s created." % (name))
    except exceptions.Conflict:
        print("%s already exists." % (name))

def delete_dataset(name):
    """
Creates a new BigQuery dataset with the selected name.
    """
    client = bigquery.Client()
    dataset_ref = client.dataset(name)
    dataset = bigquery.Dataset(dataset_ref)
    try:
        client.delete_dataset(dataset)
        print("Done, %s deleted." % name)
    except google.api_core.exceptions.BadRequest:
        print "Couldn't delete, delete tables first."

def delete_table(dataset, name):
    """
Deletes a BigQuery table with the referenced name inside the dataset.
    """
    client = bigquery.Client()
    table_ref = dataset.table(name)
    table = bigquery.Table(table_ref)
    try:
        client.delete_table(table)
        print("Done, %s deleted." % name)
    except google.api_core.exceptions.BadRequest as err:
        print("Couldn't delete: %s" % err)

def insert_data(table):
    """
Insert rows of data into a BigQuery table.
    """
    ROWS_TO_INSERT = [
        {'visit_id': 1,
        'visit_time': '2017-04-01T12:21:32',
        'payload': 
            {'visit_location': 'NORTH',
            'metadata': [
                {'key':'first_name','value':'Alice'},
                {'key':'favorite_color','value':'red'},
                {'key':'last_purchase_id','value':'1243'},
                {'key':'last_purchase_total','value':'34.53'},
            ], 'metrics': [
                {'key':'checkout_time','value': 82.4},
                {'key':'net_promoter','value': 5},
                {'key':'visit_count','value': 12},
            ]}
        },
        {'visit_id': 2,
        'visit_time': '2017-04-01T12:31:51',
        'payload': 
            {'visit_location': 'EAST_SIDE',
            'metadata': [
                {'key':'first_name','value':'Mary'},
                {'key':'favorite_color','value':'red'},
                {'key':'last_purchase_id','value':'1243'},
                {'key':'last_purchase_total','value':'34.53'},
            ], 'metrics': [
                {'key':'checkout_time','value': 23.4},
                {'key':'net_promoter','value': 6},
                {'key':'visit_count','value': 3},
            ]}
        },
        {'visit_id': 3,
        'visit_time': '2017-04-01T12:28:32',
        'payload': 
            {'visit_location': 'EAST_SIDE',
            'metadata': [
                {'key':'first_name','value':'Bob'},
                {'key':'favorite_color','value':'red'},
                {'key':'last_purchase_id','value':'1243'},
                {'key':'last_purchase_total','value':'34.53'},
            ], 'metrics': [
                {'key':'checkout_time','value': 134.4},
                {'key':'net_promoter','value': 3},
                {'key':'visit_count','value': 18},
            ]}
        }
    ]
    client = bigquery.Client()
    errors = client.create_rows(table, ROWS_TO_INSERT)
    if errors:
        print("Errors: %s" % errors)
    else:
        print("Inserted %s rows." % len(ROWS_TO_INSERT))

def query_data_with_json(dataset_name, table_name):
    """
Run a SELECT statement against a BigQuery table and print the results.
This variant uses the TO_JSON_STRING function to get back json of a struct.
    """
    client = bigquery.Client()
    QUERY = """
SELECT visit_id, visit_time, payload.visit_location, TO_JSON_STRING(payload)
FROM `%s.%s.%s` ORDER BY visit_id LIMIT 100
""" % (client.project, dataset_name, table_name)

    rows = list(client.query_rows(QUERY, timeout=30))
    for row in rows:
        print("%s\t%s\t%s\t%s" % (row[0], row[1], row[2], row[3]))

def query_data_with_repeating_element(dataset_name, table_name):
    """
Run a SELECT statement against a BigQuery table and print the results.
This variant uses sub-selects to get specific values out of the repeating
records.
    """
    client = bigquery.Client()
    QUERY = """
SELECT visit_id, visit_time, payload.visit_location,
  (SELECT value FROM UNNEST(payload.metadata) WHERE key = "first_name")
    AS first_name,
  (SELECT value FROM UNNEST(payload.metrics) WHERE key = "net_promoter")
    AS net_promoter
FROM `%s.%s.%s` ORDER BY visit_id LIMIT 100
""" % (client.project, dataset_name, table_name)

    rows = list(client.query_rows(QUERY, timeout=30))
    for row in rows:
        print("%s\t%s\t%s\t%s\t%s" % (row[0], row[1], row[2], row[3], row[4]))

def query_data_with_udf(dataset_name, table_name):
    """
Run a SELECT statement against a BigQuery table and print the results.
This query uses an in-statement UDF to do some data processing with Javascript
but you can also load JS libraries from Google Cloud Storage.
    """
    client = bigquery.Client()
    QUERY = """
CREATE TEMPORARY FUNCTION rot13(x STRING)
RETURNS STRING
LANGUAGE js
AS \"\"\"
x = x.replace(/[a-zA-Z]/g,function(c){
   return String.fromCharCode((c<='Z'?90:122)>=(c=c.charCodeAt(0)+13)?c:c-26);
});
return x;
\"\"\";
SELECT visit_id, visit_time, payload.visit_location,
  (SELECT rot13(value) FROM UNNEST(payload.metadata) WHERE key = "first_name")
    AS first_name,
  (SELECT value FROM UNNEST(payload.metrics) WHERE key = "net_promoter")
     AS net_promoter
FROM `%s.%s.%s` ORDER BY visit_id LIMIT 100
""" % (client.project, dataset_name, table_name)

    rows = list(client.query_rows(QUERY, timeout=30))
    for row in rows:
        print("%s\t%s\t%s\t%s\t%s" % (row[0], row[1], row[2], row[3], row[4]))

def query_data_into_table(dataset_name, source_table, dest_table):
    "Select data from a table into another table."
    client = bigquery.Client()
    QUERY = """
SELECT visit_id, visit_time, payload.visit_location,
  (SELECT value FROM UNNEST(payload.metadata) WHERE key = "first_name")
    AS first_name,
  (SELECT value FROM UNNEST(payload.metrics) WHERE key = "net_promoter")
    AS net_promoter
FROM `%s.%s.%s`
""" % (client.project, dataset_name, source_table)

    dataset = client.dataset(dataset_name)
    job_config = bigquery.job.QueryJobConfig()
    job_config.destination = dataset.table(dest_table)
    job_config.write_disposition = 'WRITE_TRUNCATE'
    query_job = bigquery.job.QueryJob(str(uuid.uuid4()),
        QUERY, client=client, job_config=job_config)
    query_job._begin()
    while not query_job.done():
        time.sleep(5)
    print("%s bytes processed." % query_job.total_bytes_billed)

def extract_table_to_bucket(dataset_name, table, bucket_name):
    "Select data from a table into Google Cloud Storage."
    client = bigquery.Client()
    dataset = client.dataset(dataset_name)
    table_ref = dataset.table(table)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = 'AVRO'
    dest = ['gs://%s/complex_query_output-*.avro' % bucket_name]
    query_job = bigquery.job.ExtractJob(str(uuid.uuid4()),
        table_ref, dest, client, job_config=job_config)
    # Here's an example of dumping the JSON sent to the BigQuery API
    print(query_job._build_resource())
    query_job._begin()
    while not query_job.done():
        time.sleep(5)
    if query_job.errors:
        print(query_job.errors)
    print("%s file(s) created." % query_job._job_statistics().get('destinationUriFileCounts')[0])


def get_dataset(name):
    "Quick function to get a dataset by name."
    client = bigquery.Client()
    dataset_ref = client.dataset(name)
    return(bigquery.Dataset(dataset_ref))

def get_table(dataset, name):
    "Quick function to get a table by name."
    client = bigquery.Client()
    table_ref = dataset.table(name)
    table = bigquery.Table(table_ref)
    return(client.get_table(table))

def generate_file(file_name):
    row_count = 100000
    with gzip.open(file_name, 'wb') as f:
        recordtime = datetime.now() - (timedelta(seconds=1)*100000)
        for id in xrange(0,row_count):
            record = {'visit_id': id, 'payload':{}, 'visit_time':
                recordtime.strftime("%Y-%m-%dT%H:%m:%S")}
            record['payload']['visit_location'] = \
                random.choice(['NORTH', 'SOUTHSIDE', 'BAYSIDE', 'DOWNTOWN'])
            record['payload']['metadata'] = [
                {'key':'first_name', 'value': random_name()},
                {'key':'favorite_color',
                'value': random.choice(['green', 'blue', 'yellow', 'purple'])},
                {'key':'last_purchase_id', 'value': str(id*2)},
                {'key':'last_purchase_total',
                'value': '{:,.2f}'.format(random.randrange(1,10000)/100.0)}]
            record['payload']['metrics']= [
                {'key':'checkout_time',
                'value': random.randrange(1,10000)/10.0},
                {'key':'net_promoter',
                'value': random.randrange(1,7)},
                {'key':'visit_count',
                'value': random.randrange(1,50)}]
            recordtime = recordtime + timedelta(seconds=5)
            f.write(json.dumps(record)+"\n")
    print("File generated, %s rows in %s." % (row_count, file_name))


def random_name():
    return random.choice(['Sophia','Jackson','Emma','Aiden','Olivia',
        'Lucas','Ava','Liam','Mia','Noah','Isabella','Ethan','Riley',
        'Mason','Aria','Caden','Zoe','Oliver','Charlotte','Elijah',
        'Lily','Grayson','Layla','Jacob','Amelia','Michael','Emily',
        'Benjamin','Madelyn','Carter','Aubrey','James','Adalyn',
        'Jayden','Madison','Logan','Chloe','Alexander','Harper',
        'Caleb','Abigail','Ryan','Aaliyah','Luke','Avery','Daniel',
        'Evelyn','Jack','Kaylee','William','Ella','Owen','Ellie',
        'Gabriel','Scarlett','Matthew','Arianna','Connor','Hailey',
        'Jayce','Nora','Isaac','Addison','Sebastian','Brooklyn',
        'Henry','Hannah','Muhammad','Mila','Cameron','Leah','Wyatt',
        'Elizabeth','Dylan','Sarah','Nathan','Eliana','Julian',
        'Mackenzie','Eli','Peyton','Levi','Maria','Isaiah','Grace',
        'Landon','Adeline','David','Elena','Christian','Anna',
        'Andrew','Victoria','Brayden','Camilla','John','Lillian',
        'Lincoln'])

def load_data_from_file(dataset, table, file_name):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)

    with open(file_name, 'rb') as source_file:
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'NEWLINE_DELIMITED_JSON'
        job_config.compression = 'GZIP'
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config)

    job.result()

    print('Loaded %s rows into %s:%s.' %
        (job.output_rows, dataset, table))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--create_dataset',
        help='Create a new complex_dataset dataset',
        action="store_true")
    parser.add_argument('--delete_dataset',
        help='Delete the complex_dataset dataset',
        action="store_true")
    parser.add_argument('--create_table',
        help='Create a new complex_stream_table table',
        action="store_true")
    parser.add_argument('--delete_table',
        help='Delete the complex_stream_table table',
        action="store_true")
    parser.add_argument('--insert_data',
        help='Stream some data into the complex_stream_table table',
        action="store_true")
    parser.add_argument('--query_data_json',
        help='Select data from the complex_stream_table table with json',
        action="store_true")
    parser.add_argument('--query_data_repeating',
        help='Select data from the complex_stream_table table',
        action="store_true")
    parser.add_argument('--query_data_udf',
        help='Select data from the complex_stream_table table with a udf',
        action="store_true")
    parser.add_argument('--generate_file',
        help='Generate some random JSON data to load into a table',
        action="store_true")
    parser.add_argument('--load_file',
        help='Create a load job for the complex_dataset.json.gz file',
        action="store_true")
    parser.add_argument('--query_into_table',
        help='Output Query results into different table',
        action="store_true")
    parser.add_argument('--extract_table_to_bucket',
        help='Extract a table to Google Cloud Storage',
        action="store")
    args = parser.parse_args()

    # Make sure our creds are valid.
    validate_credentials()

    # Run some functions.
    if args.create_dataset:
        # Create a dataset inside our BigQuery Project
        create_dataset('complex_dataset','Example Python Test Data')

    elif args.delete_dataset:
        # Delete a dataset inside our BigQuery Project
        delete_dataset('complex_dataset')

    elif args.create_table:
        dataset = get_dataset('complex_dataset')
        # Create a table inside our dataset
        create_table(dataset, 'complex_stream_table', 'Streaming Data Table')

    elif args.delete_table:
        dataset = get_dataset('complex_dataset')
        # Delete a table from our dataset
        delete_table(dataset, 'complex_stream_table')

    elif args.insert_data:
        dataset = get_dataset('complex_dataset')
        table = get_table(dataset, 'complex_stream_table')
        # Insert some data into the table
        insert_data(table)

    elif args.query_data_json:
        query_data_with_json('complex_dataset','complex_stream_table')

    elif args.query_data_repeating:
        query_data_with_repeating_element('complex_dataset','complex_stream_table')

    elif args.query_data_udf:
        query_data_with_udf('complex_dataset','complex_stream_table')

    elif args.generate_file:
        generate_file('complex_dataset.json.gz')

    elif args.load_file:
        load_data_from_file('complex_dataset','complex_stream_table','complex_dataset.json.gz')

    elif args.query_into_table:
        query_data_into_table('complex_dataset','complex_stream_table','complex_query_output')

    elif args.extract_table_to_bucket:
        extract_table_to_bucket('complex_dataset','complex_query_output',args.extract_table_to_bucket)

    else:
        print "Command not found, use --help for script options."
