# PyTexas-BigQuery
BigQuery with Python Presentation for PyTexas 2017

This repo contains two script that demonstrate basic usage of the Google Cloud BigQuery service.

## Account Setup

1. Sign up for a Google Cloud account, if you don't have one already: [Google Cloud Console](https://console.cloud.google.com/)
2. Create a [new project](https://console.cloud.google.com/cloud-resource-manager), or use an existing one
3. [Enable billing](https://support.google.com/cloud/answer/6293499#enable-billing) for your project
4. Create a service account from the [IAM Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts/project) page, and download the JSON file containing the private key:

![IAM Service Account Screen](/images/iam_add_service_account.png?raw=true)

Drop the JSON file into a safe place on your file system, and in your terminal, set GOOGLE_APPLICATION_CREDENTIALS to that path:

```
yourmachine:pytexas-bigquery you$ export GOOGLE_APPLICATION_CREDENTIALS="~/MyProject-1234.json"
```

5.  Create a virtual environment, and install the dependencies:

```
yourmachine:pytexas-bigquery you$ virtualenv venv
yourmachine:pytexas-bigquery you$ source venv/bin/activate
(venv) yourmachine:pytexas-bigquery you$ pip install -r requirements.txt
```

## Simple Examples

Create your BigQuery dataset, create your first table, insert some data into the table via the stream API, and then query it:

```
$ python bigquery-simple-examples.py --create_dataset
Done, simple_dataset created.
$ python bigquery-simple-examples.py --create_table
Done, simple_stream_table created.
$ python bigquery-simple-examples.py --insert_data
Inserted 6 rows.
$ python bigquery-simple-examples.py --query_data
Alice	2017-04-01 12:21:32+00:00	234	3.4
Beatrix	2017-04-01 12:25:45+00:00	564	7.2
Edgar	2017-04-01 12:56:27+00:00	268	3.1
Henry	2017-04-01 12:13:11+00:00	72	5.4
Percy	2017-04-01 12:29:19+00:00	145	6.4
Susan	2017-04-01 12:23:14+00:00	174	6.8
$ 
```

The table and data will now be viewable in the [BigQuery UI](https://bigquery.cloud.google.com/dataset/).

When you're done, you can delete the table and the dataset:

```
$ python bigquery-simple-examples.py --delete_table
Done, simple_stream_table deleted.
$ python bigquery-simple-examples.py --delete_dataset
Done, simple_dataset deleted.
```

## Complex Examples

Create your BigQuery dataset, create your first table, insert some complex data into the table via the stream API, and then query it with JSON results, row-specific results, and a UDF:

```
$ python bigquery-complex-examples.py --create_dataset
Done, complex_dataset created.
$ python bigquery-complex-examples.py --create_table
Done, complex_stream_table created.
$ python bigquery-complex-examples.py --insert_data
Inserted 3 rows.
$ python bigquery-complex-examples.py --query_data_json
1	2017-04-01 12:21:32+00:00	NORTH	{"visit_location":"NORTH","metadata":[{"key":"first_name","value":"Alice"},{"key":"favorite_color","value":"red"},{"key":"last_purchase_id","value":"1243"},{"key":"last_purchase_total","value":"34.53"}],"metrics":[{"key":"checkout_time","value":82.4},{"key":"net_promoter","value":5},{"key":"visit_count","value":12}]}
2	2017-04-01 12:31:51+00:00	EAST_SIDE	{"visit_location":"EAST_SIDE","metadata":[{"key":"first_name","value":"Mary"},{"key":"favorite_color","value":"red"},{"key":"last_purchase_id","value":"1243"},{"key":"last_purchase_total","value":"34.53"}],"metrics":[{"key":"checkout_time","value":23.4},{"key":"net_promoter","value":6},{"key":"visit_count","value":3}]}
3	2017-04-01 12:28:32+00:00	EAST_SIDE	{"visit_location":"EAST_SIDE","metadata":[{"key":"first_name","value":"Bob"},{"key":"favorite_color","value":"red"},{"key":"last_purchase_id","value":"1243"},{"key":"last_purchase_total","value":"34.53"}],"metrics":[{"key":"checkout_time","value":134.4},{"key":"net_promoter","value":3},{"key":"visit_count","value":18}]}
$ python bigquery-complex-examples.py --query_data_repeating
1	2017-04-01 12:21:32+00:00	NORTH	Alice	5.0
2	2017-04-01 12:31:51+00:00	EAST_SIDE	Mary	6.0
3	2017-04-01 12:28:32+00:00	EAST_SIDE	Bob	3.0
$ python bigquery-complex-examples.py --query_data_udf
1	2017-04-01 12:21:32+00:00	NORTH	Nyvpr	5.0
2	2017-04-01 12:31:51+00:00	EAST_SIDE	Znel	6.0
3	2017-04-01 12:28:32+00:00	EAST_SIDE	Obo	3.0
$ 
```

Now you can view that complex data in the [BigQuery UI](https://bigquery.cloud.google.com/dataset/) as well.

Then try and load some more data by generating a random 100,000 row data file, and posting it via a load data job, overwriting the data in the table.

```
$ python bigquery-complex-examples.py --generate_file
File generated, 100000 rows in complex_dataset.json.gz.
$ python bigquery-complex-examples.py --load_file
Loaded 100000 rows into complex_dataset:complex_stream_table.
$ 
```

Your 100,000 rows are now viewable in the [BigQuery UI](https://bigquery.cloud.google.com/dataset/).

You can now query that data and send the results into another table (complex_query_output):

```
$ python bigquery-complex-examples.py --query_into_table
18874368 bytes processed.
```

You can export that file into Google Cloud storage as AVRO, where the argument to the script is the name of your bucket. This example also shows you what the job creation JSON looks like:

```
$ python bigquery-complex-examples.py --extract_table_to_bucket pytexas-bigquery
{'configuration': {'extract': {'destinationFormat': 'AVRO', 'destinationUris': ['gs://pytexas-bigquery/complex_query_output-*.avro'], 'sourceTable': {'projectId': u'pytexas-bigquery', 'tableId': 'complex_query_output', 'datasetId': 'complex_dataset'}}}, 'jobReference': {'projectId': u'pytexas-bigquery', 'jobId': 'df41a4cd-e6aa-4445-99df-304c1ae46a84'}}
1 file(s) created.
```

Once you have an AVRO file in your bucket, you can import that into another table:

```
$ python bigquery-complex-examples.py --load_table_from_bucket pytexas-bigquery
Loaded 100000 rows
```
