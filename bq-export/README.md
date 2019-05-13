# BigQuery to Recommendations AI Events Exporter

Written by Ed Brocklebank (ed.brocklebank@jellyfish.co.uk)

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

3. Open the terminal window and run the following command:

	> export GOOGLE_APPLICATION_CREDENTIALS=credentials.json

This will ensure the correct credentials file is used when making requests.

4. Run the following to install the necessary libraries and create a virutal environment

	> virtualenv venv
	> source venv/bin/activate
	> pip install -r requirements.txt

## Running

Create a Storage bucket called 'recommendation-ai' (or any name you wish) using the Google Cloud UI.

Run the export with the command

	> python export.py recommendations-ai

The output JSON files will be placed into the storage bucket. 