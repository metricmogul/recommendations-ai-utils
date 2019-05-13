SELECT
    fullVisitorId,
    visitId,
    hits.hitNumber as hitNumber,
    CONCAT(hits.page.hostname, hits.page.pagePath) as url,
    TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND) as hitTime,
    hits.transaction.transactionId as transactionId,
    prods.productSKU as productId,
    prods.v2ProductName AS productName,
    prods.v2ProductCategory AS productCategory,
    prods.productQuantity AS productQty,
    prods.productListName as productListName,
    SAFE_DIVIDE(prods.productPrice, 1000000) AS productPrice,
    "GBP" as productCurrencyCode,
    hits.ecommerceaction.action_type AS actionType,
    CASE CAST(hits.ecommerceaction.action_type as INT64)
      --WHEN 1 THEN 'list-click'
      WHEN 2 THEN 'detail-page-view'
      WHEN 3 THEN 'add-to-cart'
      WHEN 4 THEN 'remove-from-cart'
      WHEN 5 THEN CONCAT('checkout-step-', CAST(hits.eCommerceAction.step as STRING))
      WHEN 6 THEN 'purchase-complete'
      ELSE 'other'
    END as eventType
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS prods
  WHERE
    CAST(hits.ecommerceaction.action_type AS INT64) IN (2, 3, 4, 5, 6)
    AND (prods.isimpression IS NULL
    OR prods.isimpression = FALSE)
  GROUP BY
    fullVisitorId,
    visitStartTime,
    visitId,
    hitNumber,
    hitTime,
    productId,
    productName,
    productPrice,
    productCategory,
    productQty,
    productCurrencyCode,
    productListName,
    actionType,
    hits.page.pagePath,
    hits.page.hostname,
    hits.eCommerceAction.step,
    hits.transaction.transactionId,
    hits.transaction.transactionRevenue	
  ORDER BY visitID, hitTime
