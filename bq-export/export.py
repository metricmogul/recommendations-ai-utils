import argparse
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

##############################################
# CONFIGURATION
# Change the settings below to suit your needs
##############################################

# The local directory containing the SQL files
SQL_DIR = 'sql'

# The list of input SQL queries to run
INPUT_FILES = ['ecommerce_events.sql', 'transactions.sql', 'pageviews.sql']

# The name of the temporary dataset and table which will be used for exports
BIGQUERY_TEMP_DATASET = 'recommendations_ai_temp'
BIGQUERY_TEMP_TABLE = 'temp_extract_table'

# Location of BigQuery table and storage
CLOUD_LOCATION = 'US'

##############################################
# END OF CONFIG
##############################################


parser = argparse.ArgumentParser()

parser.add_argument("bucket",
					help='Name of the Storage bucket to save exports to. This must be created'
						'manually before running the script.')

parser.add_argument("-t", "--temp-dataset-id",
					default=BIGQUERY_TEMP_DATASET,
					help='The name of the temporary dataset to ' \
						'create. Defaults to "{}"'.format(BIGQUERY_TEMP_DATASET))

parser.add_argument("-l", "--location",
					default=CLOUD_LOCATION,
					help='The two letter code of your BigQuery and Storage locations. ' \
						'Defaults to "{}"'.format(CLOUD_LOCATION))


def export_from_bigquery(bucket_name, dataset_id, temp_table, location):
	"""Runs a query and exports the data to a local file"""
	client = bigquery.Client()

	# Create temp dataset
	print('Created temp dataset {}'.format(dataset_id))
	try:
		dataset_ref = client.dataset(dataset_id)
		dataset = bigquery.Dataset(dataset_ref)
		dataset.location = location
		dataset = client.create_dataset(dataset)
	except Conflict as e:
		print('Unable to create dataset {}. Error was {}'.format(dataset_id, str(e)))
		exit()

	# configure the extract job
	job_config = bigquery.QueryJobConfig()
	table_ref = dataset.table(temp_table)
	job_config.destination = table_ref

	for input_file in INPUT_FILES:
		# debugging
		print('Opening file {}'.format(input_file))

		# read in the SQL command
		file = open('./{}/{}'.format(SQL_DIR, input_file), 'r')
		sql = file.read()
		file.close()

		# run query
		print('Executing query')
		job = client.query(
			sql,
			location=location,
			job_config=job_config)

		# ceate new table containing data
		job.result()

		# write table to Cloud Storage
		destination_uri = "gs://{}/{}".format(bucket_name, input_file.replace('.sql', '.json'))
		print('Writing data to cloud storage file {}'.format(destination_uri))
		extract_table_to_storage(destination_uri, dataset_id, temp_table, location)

		# Delete the temp table
		print ('Deleting temp table {}'.format(temp_table))
		client.delete_table(table_ref)

	# delete temp dataset
	print('Deleting temp dataset {}'.format(dataset_id))
	client.delete_dataset(dataset_ref, delete_contents=True)

		
def extract_table_to_storage(destination_uri, dataset_id, table, location):
    """Exports a table to a JSON file"""
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table)

    # job config
    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = 'NEWLINE_DELIMITED_JSON'
    job_config.print_header = False

    # location must match that of source table
    extract_job = client.extract_table(
    	table_ref, 
    	destination_uri,
    	location=location,
        job_config=job_config
    	)
    extract_job.result()

    print("Exported {}.{} to {}".format(dataset_id, table, destination_uri))

if __name__ == "__main__":
	args = parser.parse_args()
	export_from_bigquery(args.bucket, args.temp_dataset_id,
						 BIGQUERY_TEMP_TABLE, args.location)
