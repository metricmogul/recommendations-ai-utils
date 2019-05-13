import argparse
import json
from google.cloud import bigquery
from google.api_core.exceptions import Conflict


parser = argparse.ArgumentParser()
parser.add_argument("-e", "--environment",
					default='DEFAULT',
					help='The name of config to use. Defaults to "DEFAULT"')
args = parser.parse_args()


def export_from_bigquery(ga_dataset, bucket_name, input_files,
						 dataset_id, temp_table, location, lookback_days):
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

	query_params = [
		bigquery.ScalarQueryParameter("lookback_days", "STRING", lookback_days)
	]

	# configure the extract job
	job_config = bigquery.QueryJobConfig()
	job_config.query_parameters = query_params
	table_ref = dataset.table(temp_table)
	job_config.destination = table_ref

	try:
		for input_file in input_files:
			# debugging
			print('Opening file {}'.format(input_file))

			# read in the SQL command
			file = open('./{}/{}'.format(config[args.environment]['SQL_DIR'], input_file), 'r')
			sql = file.read()
			sql = sql.replace('{{dataset}}', ga_dataset)
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

	finally:
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
	with open('config.json', 'r') as f:
		config = json.load(f)

	export_from_bigquery(config[args.environment]['BIGQUERY_GA_DATASET'],
						 config[args.environment]['STORAGE_BUCKET'],
						 config[args.environment]['SQL_INPUT_FILES'],
						 config[args.environment]['BIGQUERY_TEMP_DATASET'],
						 config[args.environment]['BIGQUERY_TEMP_TABLE'],
						 config[args.environment]['CLOUD_LOCATION'],
						 config[args.environment]['LOOKBACK_DAYS'])
