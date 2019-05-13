#standardSQL
SELECT
    CASE 
      WHEN hits.page.pagePath = '/home' THEN 'home-page-view'
      WHEN REGEXP_CONTAINS(hits.page.pagePath, '(^/?$)') THEN 'home-page-view'
      WHEN REGEXP_CONTAINS(hits.page.pagePath, 'cart') THEN 'shopping-cart-page-view'
      WHEN REGEXP_CONTAINS(hits.page.pagePath, 'search') THEN 'search'
      ELSE 'other'
    END as eventType,
    ARRAY_AGG(STRUCT(fullVisitorId as visitorId)) as userInfo,
    ARRAY_AGG(STRUCT(CONCAT(hits.page.hostname, hits.page.pagePath) as uri)) as eventDetail,
    ARRAY_AGG(STRUCT(hits.page.searchKeyword as searchQuery)) as productEventDetail,
    FORMAT_TIMESTAMP('%FT%H:%M:%E9SZ', TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND)) as eventTime
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
    UNNEST(hits) as hits
  WHERE
    hits.type = 'PAGE'
    AND REGEXP_CONTAINS(hits.page.pagePath, '(^/?$|/home|cart|search)')
  GROUP BY fullVisitorId, visitId, hits.page.hostname, hits.page.pagePath, hits.page.searchKeyword, visitStartTime, hits.time
  ORDER BY fullVisitorId
