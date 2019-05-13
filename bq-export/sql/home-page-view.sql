SELECT
    "home-page-view" eventType,
    ARRAY_AGG(STRUCT(fullVisitorId as visitorId)) as userInfo,
    ARRAY_AGG(STRUCT(CONCAT(hits.page.hostname, hits.page.pagePath) as uri)) as eventDetail,
    FORMAT_TIMESTAMP('%FT%H:%M:%E9SZ', TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND)) as eventTime
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) as hits
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) AND
    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND
    hits.type = 'PAGE' AND
    REGEXP_CONTAINS(hits.page.pagePath, '(^/?$|/home)')
  GROUP BY fullVisitorId, visitId, hits.page.hostname, hits.page.pagePath, hits.page.searchKeyword, visitStartTime, hits.time
  ORDER BY fullVisitorId
