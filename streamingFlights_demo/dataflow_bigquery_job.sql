SELECT
  *,
  TIMESTAMP_SECONDS(contact) AS contact_bqtime,
  TIMESTAMP_SECONDS(time) AS time_bqtime,
  PARSE_DATETIME("%Y-%m-%d %H:%M:%S",query_time_bq) AS query_time_bqtime
FROM TOPICID;