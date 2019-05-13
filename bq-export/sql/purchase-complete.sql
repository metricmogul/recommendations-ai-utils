WITH product_event_detail AS (
  SELECT
   fullVisitorId as visitorId,
   [STRUCT(hits.transaction.transactionId as id, 
   SAFE_DIVIDE(hits.transaction.transactionRevenue, 1000000) as revenue, 
   hits.transaction.currencyCode)] as purchaseTransaction,
   ARRAY(SELECT STRUCT(
      productSKU as id,
      hits.transaction.currencyCode as currencyCode,
      SAFE_DIVIDE(productPrice, 1000000) as originalPrice, 
      CAST(productQuantity as INT64) as quantity
    ) from UNNEST(hits.product)) as productDetails,
  FORMAT_TIMESTAMP('%FT%H:%M:%E9SZ', TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND)) as eventTime
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) AND
    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND
    hits.type = 'PAGE' AND
    hits.eCommerceAction.action_type = '6' AND
    hits.transaction.transactionRevenue is not null
)

SELECT
  "purchase-complete" as eventType,
  STRUCT(visitorId) as userInfo,
  STRUCT(purchaseTransaction, productDetails) as productEventDetail,
  eventTime
FROM product_event_detail
