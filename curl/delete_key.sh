#!/bin/bash
# Run with ./register_key.sh /path/to/credentials.json [project_id] [api_key]

# Check params
if [ "$#" -ne 3 ]; then
  echo "Usage: register_key.sh /path/to/credentials.json [project_id] [api_key]" >&2
  exit 1
fi

# Set up variables
export GOOGLE_APPLICATION_CREDENTIALS=$1
PROJECT_NUMBER=$2
API_KEY=$3

# Register the key
curl -X DELETE -G \
     -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
     -H "Content-Type: application/json; charset=utf-8" \
     https://recommendationengine.googleapis.com/v1beta1/projects/$PROJECT_NUMBER/locations/global/catalogs/default_catalog/eventStores/default_event_store/predictionApiKeyRegistrations/$API_KEY