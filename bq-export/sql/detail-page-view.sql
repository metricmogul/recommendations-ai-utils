WITH product_event_detail AS (
  SELECT
   fullVisitorId as visitorId,
   ARRAY(SELECT STRUCT(
      productSKU as id,
      hits.transaction.currencyCode as currencyCode,
      SAFE_DIVIDE(productPrice, 1000000) as originalPrice
    ) from UNNEST(hits.product)) as productDetails,
  FORMAT_TIMESTAMP('%FT%H:%M:%E9SZ', TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND)) as eventTime
  FROM
    `{{dataset}}.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)) AND
    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND
    hits.eCommerceAction.action_type = '2'
)

SELECT
  "detail-page-view" as eventType,
  STRUCT(visitorId) as userInfo,
  STRUCT(productDetails) as productEventDetail,
  eventTime
FROM product_event_detail
