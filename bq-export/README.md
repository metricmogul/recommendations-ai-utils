# BigQuery to Recommendations AI Events Exporter

## Aim

This script exports data from Google Analytics session tables in BigQuery and converts
it to a JSON format suitable for injesting into Recommendations AI for model training. The
outputted files are saved to Google Storage.

The Recommendations AI event format is described here:
<https://cloud.google.com/recommendations-ai/docs/reference/rest/v1beta1/UserEvent>

## Setup

1. Create a service account within the Google Cloud Console. Give it permissions 
BigQuery Job User, BigQuery Data Editor and Storage Admin.

2. Download the JSON credentials file. Rename it credentials.json and place it in
the root directory (the same one this file is in).

3. Run the following to install the necessary libraries and create a virutal environment

    ```
    > virtualenv venv
    > source venv/bin/activate
    > pip install -r requirements.txt
    ```

4. Open the terminal window and run the following command:

	`export GOOGLE_APPLICATION_CREDENTIALS=credentials.json`

    This will ensure the correct credentials file is used when making requests.

## Running

Create a Storage bucket called 'recommendation-ai' (or any name you wish) using the Google Cloud UI.

Open the config.json file and edit the following variables:

- _SQL_INPUT_FILES_: Add or remove sql commands to run
- _STORAGE_BUCKET_: Should be set to the name of the storage bucket created in the step above. This
is where the output JSON files will be stored
- _BIGQUERY_GA_DATASET_: The name of the dataset holding your ga_sessions_* tables.
- _BIGQUERY_TEMP_DATASET_: The name of the temporary dataset which will be created when exporting your data.
It is recommended you leave the default value.
- _BIGQUERY_TEMP_TABLE_: The name of the temporary table which will be created when exporting your data.
It is recommended you leave the default value.
- _CLOUD_LOCATION_: The two letter country code of the location of your Storage bucket.
- _LOOKBACK_DAYS_: The number of days worth of data to extract. For example, if set to 90, the past 90
days of events will be exported.

Run the export with the command

	python export.py

For example

    python export.py bigquery-public-data.google_analytics_sample recomendations-ai

The output JSON files will be placed into the storage bucket. 

For more config options type:

    python export.py -h
    