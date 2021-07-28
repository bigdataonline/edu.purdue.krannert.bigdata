SELECT
    *,
    JSON_EXTRACT(text,'$.text') text_text,
    JSON_EXTRACT(text,'$.extended_tweet.full_text') text_extended
FROM twitter.tweets;