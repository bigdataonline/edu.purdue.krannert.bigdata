# Twitter Streaming

This directory has code and schemas for interacting with messages and 
metadata from Twitter. Twitter feeds are nested with user metadata embedded in
the record for a tweet, or an original tweet embedded in a retweet.

In order to use the tweets for analysis, we flatten the nested structure of
the messages, separating out the user metadata as individual records. The
users are written to a separate location (either a GCS bucket or a Pub/Sub
topic) than the tweets.

The python folder contains code to use with Cloud Function that will pull the 
latest data from the Twitter API and store each tweet and each user as 
separate files in GCS buckets. 

The schema folder has a definition for data content for use by BigQuery when
reading data from a Pub/Sub topic in DataFlow.

## API Code

The structure of the nested tweets is fully described at: 
[Twitter Developer Platform: Docs](https://developer.twitter.com/en/docs/twitter-api) for more on the API.

To use the Python code, you need to have consumer and app keys provided by
twitter in order to access the twitter API.
1. First, go through the process of creating a developer account with 
   twitter at [Twitter: Apply for Standard product track](https://developer.twitter.com/en/apply-for-access.html). Twitter will provide you with a consumer key and secret
   in the process (basically a user ID and password). You are also given a
   bearer token.
   During the application, identify that you are a student using this for 
   educational purposes.
2. Once you have a developer account, you can generate an access token, which
   is another pair of key and secret.
3. When you upload main.py and requirements.txt to a Cloud Function, create a 
   file named twitterKeys.json with all of the above keys, as:
   
> {
>  "consumer_key": "USER KEY",
>  "consumer_secret": "USER SECRET KEY",
>  "bearer_token": "BEARER TOKEN",
>  "access_token": "APP KEY",
>  "access_secret": "APP SECRET KEY"
> }

## Schemas

The schema folder has a schema used when adding a Pub/Sub topic as a data
source to BigQuery (when using the DataFlow interface). This schema defines
how DataFlow is to parse the messages in the Pub/Sub topic.

Note that there are other fields within the tweets and twitter users that are
not extracted by the code which you may find interesting, such as the actual
text of the tweets. The tweet text is in two locations, an element named 
"text" and a field named "full_text" within the "extended_tweet" element. 
The entire tweet is extracted by the code and can be found in the "text" field
(sorry for the confusion since twitter has a 'text' field in its tweets.)

You can easily extract the text fields from the tweets in BigQuery as:

> SELECT 
>    *,
>    JSON_EXTRACT(text,'$.text') text_text, 
>    JSON_EXTRACT(text,'$.extended_tweet.full_text') text_extended
>FROM twitter.tweets;

_(where twitter is your dataset name and tweets is your table name.)_