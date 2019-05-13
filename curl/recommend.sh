# Check params
if [ "$#" -ne 3 ]; then
  echo "Usage: recommend.sh /path/to/credentials.json [project_id] [api_key]" >&2
  exit 1
fi

# Set up variables
export GOOGLE_APPLICATION_CREDENTIALS=$1
PROJECT_NUMBER=$2
API_KEY=$3

curl -X POST \
    -H "Content-Type: application/json; charset=utf-8" \
    --data  '{
          "filter": "filterOutOfStockItems tag=\"springsale\"",
          "dryRun": true,
          "userEvent": {
               "eventType": "detail-page-view",
               "userInfo": {
                    "visitorId": "visitor1",
                    "userId": "user1",
                    "ipAddress": "0.0.0.0",
                    "userAgent": "Mozilla/5.0 (Windows NT 6.1)"
               },
              "eventDetail": {
                  "experimentIds": "experiment-group"
               },
              "productEventDetail": {
                  "productDetails": [
                       {
                        "id": "123456"
                       }
                  ]
              }
         }
     }' https://recommendationengine.googleapis.com/v1beta1/projects/$2/locations/global/catalogs/default_catalog/eventStores/default_event_store/placements/product_detail:predict?key=$3
