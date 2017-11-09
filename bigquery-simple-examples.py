#!/usr/bin/env python

from google.cloud import bigquery
from google.api_core import exceptions
import argparse

def validate_credentials():
    """
Check and see if we have a valid credentials file,
just to save folks some heartache.
    """

    client = bigquery.Client()
    try:
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
        bigquery.SchemaField('full_name', 'STRING',
            mode='required', description="Visitor's Name"),
        bigquery.SchemaField('visit_time', 'TIMESTAMP',
            mode='required', description="Visit Time"),
        bigquery.SchemaField('visit_length', 'INT64',
            mode='required', description="Length of Visit in Seconds"),
        bigquery.SchemaField('sentiment', 'FLOAT64',
            mode='required', description="Calculated Happiness Score"),
    ]
    client = bigquery.Client()
    table_ref = dataset.table(name)
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table.description = description
    try:
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
Insert 6 rows of data into a BigQuery table.
    """
    ROWS_TO_INSERT = [
        (u'Alice', '2017-04-01T12:21:32', 234, 3.4),
        (u'Susan', '2017-04-01T12:23:14', 174, 6.8),
        (u'Beatrix', '2017-04-01T12:25:45', 564, 7.2),
        (u'Henry', '2017-04-01T12:13:11', 72, 5.4),
        (u'Edgar', '2017-04-01T12:56:27', 268, 3.1),
        (u'Percy', '2017-04-01T12:29:19', 145, 6.4)
    ]

    client = bigquery.Client()
    errors = client.create_rows(table, ROWS_TO_INSERT)
    if errors:
        print("Errors: %s" % errors)
    else:
        print("Inserted %s rows." % len(ROWS_TO_INSERT))

def query_data(dataset_name, table_name):
    """
Run a SELECT statement against a BigQuery table and print the results.
    """
    client = bigquery.Client()
    QUERY = """
SELECT full_name, visit_time, visit_length, sentiment
FROM `%s.%s.%s`
ORDER BY full_name
LIMIT 100
""" % (client.project, dataset_name, table_name)

    rows = list(client.query_rows(QUERY, timeout=30))
    for row in rows:
        print("%s\t%s\t%s\t%s" % (row[0], row[1], row[2], row[3]))

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--create_dataset',
        help='Create a new simple_dataset dataset',
        action="store_true")
    parser.add_argument('--delete_dataset',
        help='Delete the simple_dataset dataset',
        action="store_true")
    parser.add_argument('--create_table',
        help='Create a new simple_stream_table table',
        action="store_true")
    parser.add_argument('--delete_table',
        help='Delete the simple_stream_table table',
        action="store_true")
    parser.add_argument('--insert_data',
        help='Stream some simple lines of data into a table',
        action="store_true")
    parser.add_argument('--query_data',
        help='Select some data from a table',
        action="store_true")
    args = parser.parse_args()

    # Make sure our creds are valid.
    validate_credentials()

    # Run some functions.
    if args.create_dataset:
        # Create a dataset inside our BigQuery Project
        create_dataset('simple_dataset','Example Python Test Data')

    elif args.delete_dataset:
        # Delete a dataset inside our BigQuery Project
        delete_dataset('simple_dataset')

    elif args.create_table:
        dataset = get_dataset('simple_dataset')
        # Create a table inside our dataset
        create_table(dataset, 'simple_stream_table', 'Streaming Data Table')

    elif args.delete_table:
        dataset = get_dataset('simple_dataset')
        # Delete a table from our dataset
        delete_table(dataset, 'simple_stream_table')

    elif args.insert_data:
        dataset = get_dataset('simple_dataset')
        table = get_table(dataset, 'simple_stream_table')
        # Insert some data into the table
        insert_data(table)

    elif args.query_data:
        # Select some data from the table
        query_data('simple_dataset','simple_stream_table')

    elif args.load_simple_data:
        stream_simple_data()
    else:
        print "Command not found, use --help for script options."
