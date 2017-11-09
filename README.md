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
(venv) yourmachine:pytexas-bigquery you$ python bigquery-simple-examples.py --create_dataset
Done, simple_dataset created.
(venv) yourmachine:pytexas-bigquery you$ python bigquery-simple-examples.py --create_table
Done, simple_stream_table created.
(venv) yourmachine:pytexas-bigquery you$ python bigquery-simple-examples.py --insert_data
Inserted 6 rows.
(venv) yourmachine:pytexas-bigquery you$ python bigquery-simple-examples.py --query_data
Alice	2017-04-01 12:21:32+00:00	234	3.4
Beatrix	2017-04-01 12:25:45+00:00	564	7.2
Edgar	2017-04-01 12:56:27+00:00	268	3.1
Henry	2017-04-01 12:13:11+00:00	72	5.4
Percy	2017-04-01 12:29:19+00:00	145	6.4
Susan	2017-04-01 12:23:14+00:00	174	6.8
(venv) yourmachine:pytexas-bigquery you$ 
```

The table and data will now be viewable in the [BigQuery UI](https://bigquery.cloud.google.com/dataset/).

## Complex Examples

