 SELECT
    fullVisitorId,
    visitId,
    CONCAT(hits.page.hostname, hits.page.pagePath) as url,
    TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND) as hitTime,
    hits.transaction.transactionId as transactionId,
    hits.transaction.transactionRevenue / 1000000 as transactionRevenue
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
    UNNEST(hits) AS hits
  WHERE
    CAST(hits.ecommerceaction.action_type AS INT64) = 6 AND
    hits.transaction.transactionRevenue is not null

