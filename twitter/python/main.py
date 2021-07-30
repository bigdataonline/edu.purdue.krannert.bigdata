# This code is designed to run within a Google Cloud Function or from the command line.
# Google Cloud Function:
#   It expects a request with a message that contains a query and optionally a limit on the number of tweets to gather.
#   Example requests:
#     {"query":"olympics","bucket":"mgmt59000_twitter_tweets","user_bucket":"mgmt59000_twitter_users","debug":10,"limit":10}
#     -- This will search for 10 twitters with the term "olympics", writing the tweets to a bucket named mgmt59000_twitter_tweets
#        and the twitter users to a bucket named mgmt59000_twitter_users.
#     {"query":["olympics","swim-dive set"],"projectId":"helical-ranger-294523","topic":"twitter_tweets","userTopic":"twitter_users","bucket":"mgmt59000_twitter_tweets",
#     "userBucket":"mgmt59000_twitter_users","debug":10,"limit":10}
#     -- This will both publish in a pub/sub and store in GCS.
#     -- The query can take an array of words or phrases.
#     {"query":["olympics","tennis"],"projectId":"helical-ranger-294523","bucket":"mgmt59000_twitter_tweets","userBucket":"mgmt59000_twitter_users","path":"delimited","debug":10,"limit":100,"delim":"|"}
#     {"query":["olympics","upset"],"projectId":"helical-ranger-294523","bucket":"mgmt59000_twitter_tweets","userBucket":"mgmt59000_twitter_users","path":"arrays","debug":10,"limit":100}
# Command Line:
#   Give a query and optionally supply a limit.
#   Example calls:
#     -query olympics "swim-dive set"  -limit 25 -bucket mgmt59000_twitter_tweets -user_bucket mgmt59000_twitter_users
#     This example will search for 25 twitters mentioning olympics and "swim-dive set" and will write the filtered tweets to mgmt59000_twitter_tweets
#     and any twitter users who wrote the tweets have metadata written to mgmt59000_twitter_users.
import logging
import time
import re
import datetime
import json
import argparse

import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

from google.cloud.exceptions import Forbidden
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(format='%(asctime)s.%(msecs)03dZ,%(pathname)s:%(lineno)d,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger(__name__)

class ExampleRequest(object):
  args = None
  
  # (args.projectId,args.query,limit=args.limit,topic=args.topic,debug=args.log)
  def __init__(self, projectId, query, limit=None, topic=None, userTopic=None, bucket=None, userBucket=None, pathInBuckets=None,
               delim=None,
               debug=None):
    if type(query) == str:
      if not query.startswith('"'): query = '"' + query + '"'
    else:
      query = json.dumps(query)
    message = {'query': query, 'limit': limit if limit is not None else '', 'projectId': projectId,
               'topic': topic if topic is not None else '',
               'bucket': bucket if bucket is not None else '',
               'userTopic': userTopic if userTopic is not None else '',
               'userBucket': userBucket if userBucket is not None else ''}
    if pathInBuckets is not None: message['path']=pathInBuckets
    if delim is not None: message['delim']=delim
    if debug is not None: message['debug']=debug
    self.args = {'message': json.dumps(message)}
  
  def get_json(self, force=False):
    return json.loads(self.args['message'])

def _getMessageJSON(request):
  request_json = request.get_json(force=True)
  message = None
  if request.args is not None:
    _logger.info('request is ' + str(request) + ' with args ' + str(request.args))
    if 'message' in request.args:
      message = request.args.get('message')
    elif 'query' in request.args:
      message = request.args
  if message is None and request_json is not None:
    _logger.debug('request_json is ' + str(request_json))
    if 'message' in request_json:
      message = request_json['message']
    elif 'query' in request_json:
      message = request_json
  
  if message is None:
    print('message is empty. request=' + str(request) + ' request_json=' + str(request_json))
    message = '{"query":"The Spicy Amigos"}'
  
  if type(message) == str:
    try:
      messageJSON = json.loads(message)
    except:
      messageJSON = {"query": [message]}
  else:
    messageJSON = message
  return messageJSON

class MyListener(StreamListener):
  """Custom StreamListener for streaming data."""
  
  _tweetFields = ["created_at", "id", "id_str", "text", "in_reply_to_status_id", "in_reply_to_status_id_str",
                  "in_reply_to_user_id", "in_reply_to_user_id_str", "in_reply_to_screen_name", "geo", "coordinates",
                  "place", "contributors", "quoted_status_id", "quoted_status_id_str", "is_quote_status", "quote_count",
                  "reply_count", "retweet_count", "favorite_count", "favorited", "retweeted", "lang", "timestamp_ms"]
  _tweetReferences = {"user": "id",
                      "retweeted_status": "id",
                      "hashtags": "text",
                      "user_mentions": "id",
                      "symbols": "text",
                      "extended_tweet": "full_text"}
  _userFields = ["id", "id_str", "name", "screen_name", "location", "description", "followers_count", "friends_count",
                 "listed_count", "favourites_count", "statuses_count", "created_at", "following", "follow_request_sent",
                 "notifications"]
  
  _multivalueTweetFields=["coordinates", "hashtags", "user_mentions", "symbols", "extended_tweet"]
  
  @classmethod
  def extractReference(cls, outerField, element):
    '''

    :param outerField: the field name of a nested field which holds information for entities the tweet is referencing.
    :param element: either a dict representing one referenced entity or a list of referenced entities.
    :return: returns a list of 0,1, or more entities referenced within element.
    '''
    entities = []
    if outerField in cls._tweetReferences:
      subfield = cls._tweetReferences[outerField]
      if type(element) == dict:
        if subfield in element: entities.append(element[subfield])
      elif type(element) == list:
        for subelement in element:
          entities.extend(cls.extractReference(outerField, subelement))
    return entities
  
  @classmethod
  def extractTweet(cls, tweet, query, delim=None):
    '''
    Will process a single tweet and produce one or more records of tweet data. A single tweet may reference a tweet that it is retweeting, in which case this method returns both as tweet data records.
    :param tweet: the JSON from twitter representing one tweet.
    :param query: query that the tweet is a search result for.
    :return: returns one or more records of tweet data.
    '''
    if 'tweet' in tweet: return cls.extractTweet(tweet['tweet'], query)
    tweetRows = []
    tweetRow = {}
    for field, value in tweet.items():
      if value is not None:
        if field in cls._tweetFields:
          tweetRow[field] = value
        elif field in ['user', 'retweeted_status']:
          # These nested elements only contain one object.
          referencedEntities = cls.extractReference(field, value)
          if len(referencedEntities) > 0:
            tweetRow[field] = referencedEntities[0]
        elif field in cls._tweetReferences:
          # Capture potentially multiple references.
          referencedEntities = cls.extractReference(field, value)
          if len(referencedEntities) > 0:
            tweetRow[field] = referencedEntities
        elif field == 'entities':
          # Unnest the entities object.
          for entityType, entity in value.items():
            # Capture each referenced entity.
            referencedEntities = cls.extractReference(entityType, entity)
            if len(referencedEntities) > 0:
              tweetRow[entityType] = referencedEntities
        
        if field == 'retweeted_status':
          try:
            nestedTweets = cls.extractTweet(tweet['retweeted_status'], query,delim=delim)
            tweetRows.extend(nestedTweets)
          except:
            _logger.error('SKIPPING Cannot parse nested tweet ' + str(tweet['retweeted_status']),exc_info=True,stack_info=True)
    tweetRow['query'] = query
    if delim is None:
      # Convert empty fields for the multivalue fields into empty arrays for BigQuery since REPEATED type fields cannot be null.
      for multivalueField in MyListener._multivalueTweetFields:
        if multivalueField not in tweetRow or tweetRow[multivalueField] is None: tweetRow[multivalueField]=[]
    tweetRow['raw'] = json.dumps(tweet)
    if delim is not None:
      # Convert multivalue fields into strings with values separated by delim.
      tweetRows.append(dict(map(lambda item:
                               (
                                 item[0],
                                 delim.join(map(str,item[1])) if type(item[1])==list else item[1]
                               ),
                               tweetRow.items())))
    else:
      tweetRows.append(tweetRow)
    return tweetRows
  
  @classmethod
  def _extractUser(cls, userData):
    userRow = {}
    for field, value in userData.items():
      if value is not None:
        if field in cls._userFields:
          userRow[field] = value
    userRow['text'] = json.dumps(userData)
    return userRow
  
  @classmethod
  def extractUsers(cls, tweet):
    userRows = []
    if type(tweet) == dict:
      if 'tweet' in tweet: return cls.extractUser(tweet['tweet'])
      for field, value in tweet.items():
        if value is not None:
          if field == 'user':
            userRows.append(cls._extractUser(value))
          elif type(value) in [dict, list]:
            userRows.extend(cls.extractUsers(value))
    elif type(tweet) == list:
      for element in tweet:
        userRows.extend(cls.extractUsers(element))
    return userRows
  
  def __init__(self, projectId, query, limit, topic=None, userTopic=None, bucket=None, userBucket=None, pathInBucket=None, delim=None, debug=None):
    '''
    :param projectId:
    :param query:
    :param limit:
    :param topic:
    :param userTopic:
    :param bucket:
    :param userBucket:
    :param pathInBucket:
    :param delim:
    :param debug:
    '''
    if debug is not None: _logger.setLevel(min(debug,_logger.level))
    
    self.query = query
    self.limit = limit
    if topic is not None:
      if projectId is None: raise Exception(
        'Must supply a project ID if you want to publish to topic "{topic}".'.format(topic=topic))
      self._topic = ('projects/' + projectId + '/topics/' + topic)
      _logger.debug('Output to Pub/Sub: ' + self._topic)
    else:
      self._topic = None
    if userTopic is not None:
      if projectId is None: raise Exception(
        'Must supply a project ID if you want to publish to topic "{topic}".'.format(topic=userTopic))
      self._userTopic = ('projects/' + projectId + '/topics/' + userTopic)
      _logger.debug('Output user data to Pub/Sub: ' + self._userTopic)
    else:
      self._userTopic = None
    self._publisher = None
    self._userPublisher = None
    
    self._path=pathInBucket
    self._bucketClient = None
    self._userBucketClient = None
    self._bucket = bucket
    if bucket is not None:
      _logger.debug('Output to bucket: ' + self._bucket)
    self._userBucket = userBucket
    if userBucket is not None:
      _logger.debug('Output user data to bucket: ' + self._userBucket)
      
    self._delim=delim
  
  def _writeToBucket(self, bucketClient, bucket, records):
    key = self._createObjectKey()
    try:
      if bucketClient is None: bucketClient = storage.Client().bucket(bucket)
      for record in records:
        recordKey = key + (('_' + str(record['id'])) if 'id' in record else '')
        bucketClient.blob(recordKey).upload_from_string(json.dumps(record))
    except Forbidden as fe:
      try:
        _logger.error('Failed to write to GCS bucket {bucket} because access to object {objectName} is forbidden. Error code={code}, response content={response}'.format(
            bucket=self._bucket,
            objectName=key,
            code=str(fe.code),
            response=fe.response.content
          ),exc_info=True,stack_info=True)
      except:
        _logger.error('Failed to write to GCS bucket {bucket} because access to object {objectName} is forbidden.'.format(
            bucket=self._bucket,
            objectName=key
          ),exc_info=True,stack_info=True)
    except:
      _logger.error('Failed to write to GCS bucket {bucket}, object {objectName}.'.format(
        bucket=self._bucket,
        objectName=key
      ),exc_info=True,stack_info=True)
    return bucketClient
  
  def _createObjectKey(self):
    key = ''
    if type(self.query) == str:
      key += self.query
    else:
      key += '_'.join(self.query)
    key += '_' + str(datetime.datetime.now()) + '.json'
    return ('' if self._path is None else self._path+'/')+re.sub(r'[^A-Za-z0-9_.-]', '_', key)
  
  def on_data(self, data):
    print(json.dumps({'log': 'Found tweet...'}))
    try:
      tweets = json.loads(data)
      tweetRecords = self.extractTweet(tweets, self.query, delim=self._delim)
      userRecords = self.extractUsers(tweets)
      
      if self._bucket is not None:
        self._bucketClient = self._writeToBucket(self._bucketClient, self._bucket, tweetRecords)
      if self._userBucket is not None:
        self._userBucketClient = self._writeToBucket(self._userBucketClient, self._userBucket, userRecords)
      
      if self._topic is not None:
        if self._publisher is None: self._publisher = PublisherClient()
        for record in tweetRecords:
          try:
            self._publisher.publish(self._topic, data=json.dumps(record).encode("utf-8"), **record)
          except:
            self._publisher.publish(self._topic, data=json.dumps(record).encode("utf-8"), query=self.query)
      
      if self._userTopic is not None:
        if self._userPublisher is None: self._userPublisher = PublisherClient()
        for record in userRecords:
          try:
            self._userPublisher.publish(self._userTopic, data=json.dumps(record).encode("utf-8"), **record)
          except:
            self._userPublisher.publish(self._userTopic, data=json.dumps(record).encode("utf-8"))
      
      self.limit -= 1
      if self.limit <= 0: return False
      return True
    except:
      _logger.error('Error in on_data. Sleeping for 5 seconds.',exc_info=True,stack_info=True)
      time.sleep(5)
    return True
  
  def on_error(self, status):
    if status == 420:
      _logger.warning('Exceeding rate limit. Exiting stream.')
      return False
    _logger.error('Received error when querying for {query}: {code}. Sleeping for 10 seconds.'.format(query=self.query, code=status),exc_info=True,stack_info=True)
    time.sleep(10)
    return True

def main(request):
  """Responds to any HTTP request.
  Args:
      request (flask.Request): HTTP request object.
  Returns:
      The response text or any set of values that can be turned into a
      Response object using
      `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
  """
  messageJSON = _getMessageJSON(request)

  debug=messageJSON.get('debug', None)
  if debug is not None: _logger.setLevel(debug)

  _logger.info('Trigger message received is ' + json.dumps(messageJSON))

  limit = messageJSON.get('limit', None)
  if limit is None or str(limit) == '': limit = 10
  
  query = messageJSON.get('query', ['The Fast Dog'])
  _logger.info('Query is "{query}", limit is set to {limit}.'.format(query=query,limit=str(limit) if limit is not None else 'max'))
  
  pathInBuckets=messageJSON.get('path',None)
  
  bucket = messageJSON.get('bucket', None)
  if bucket == '': bucket = None
  if bucket is not None:
    _logger.info('Using gs://{bucket}{pathInBucket} for storing tweets.'.format(bucket=bucket,pathInBucket='' if pathInBuckets is None else '/'+pathInBuckets))
  userBucket = messageJSON.get('userBucket', None)
  if userBucket == '': userBucket = None
  if userBucket is not None:
    _logger.info('Using gs://{bucket}{pathInBucket} for storing users.'.format(bucket=userBucket,pathInBucket='' if pathInBuckets is None else '/'+pathInBuckets))

  projectId = messageJSON.get('projectId', '')
  if projectId == '': projectId = None

  topic = messageJSON.get('topic', '')
  if topic == '': topic = None
  if topic is not None:
    if projectId is None:
      _logger.error('Must include a project ID if you include a topic.')
      return 'Error attempting to access Pub/Sub topic with no project ID.'
    _logger.info('Will submit tweets to {topic}.'.format(topic=topic))
  userTopic = messageJSON.get('userTopic', '')
  if userTopic == '': userTopic = None
  if userTopic is not None:
    if projectId is None:
      _logger.error('Must include a project ID if you include a topic.')
      return 'Error attempting to access Pub/Sub topic with no project ID.'
    _logger.info('Will submit users to {topic}.'.format(topic=userTopic))

  delim=messageJSON.get('delim',None)
  if delim is not None: _logger.info('Will output multivalue fields as strings delimited by "{delim}".'.format(delim=delim))
  
  if type(query) == str: query = [query]
  
  # Set up Twitter authorization.
  # Read key and secret from file.
  with open('twitterKeys.json') as twitterKeyFile:
    keys=json.load(twitterKeyFile)
  
  requiredKeys=['consumer_key','consumer_secret','access_token','access_secret']
  if any(filter(lambda key:key not in keys,requiredKeys)):
    _logger.error('Cannot read required keys from twitterKeys.json. This file must exist and have the format {"consumer_key":"...","consumer_secret":"...","access_token":"...","access_secret":"..."}.')
    return 'Cannot read required keys from twitterKeys.json'
  twitterAuth = OAuthHandler(keys['consumer_key'], keys['consumer_secret'])
  twitterAuth.set_access_token(keys['access_token'], keys['access_secret'])
  
  for term in query:
    _logger.debug('Checking rate limit. Will potentially wait until rate limit replenishes...')
    tweepy.API(twitterAuth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    _logger.debug('Querying for {term}'.format(term=term))
    twitter_stream = Stream(twitterAuth, MyListener(projectId, term, limit, topic=topic, userTopic=userTopic, bucket=bucket,
                                             userBucket=userBucket,pathInBucket=pathInBuckets,delim=delim,debug=debug))
    twitter_stream.filter(track=term)
  return json.dumps(messageJSON)+' completed.'

if __name__ == '__main__':
  # To call from the command line, provide two arguments: query, limit.
  parser = argparse.ArgumentParser()
  
  # add arguments to the parser
  parser.add_argument('-query', nargs='+', help='One or more terms to search form (enclose phrases in double quotes.)')
  parser.add_argument('-limit', nargs='?', help='Limit the number of calls to twitter per query term, default is 10.',
                      default=None,type=int)
  parser.add_argument('-bucket',
                      help='Provide a bucket name to store the twitter data to if you want to persist the data in GCS. The bucket must already exist.',
                      default=None)
  parser.add_argument('-userBucket',
                      help='Provide a bucket name to store the user data to if you want to persist the data in GCS. The bucket must already exist.',
                      default=None)
  parser.add_argument('-projectId',
                      help='Specify the Google cloud project ID (required when publishing to pub/sub.).',
                      default=None)
  parser.add_argument('-topic',
                      help='Specify a pub/sub topic to publish to if you want the twitter records to go to pub/sub.',
                      default=None)
  parser.add_argument('-userTopic',
                      help='Specify a pub/sub topic to publish to if you want the user records to go to pub/sub.',
                      default=None)
  parser.add_argument('-delim',
                      help='Specify a delimiter to use for multivalue fields. This causes multivalue fields to be string type instead of arrays.',
                      default=None)
  parser.add_argument('-path',
                      help='Place all tweets and users within the given path (in the tweet and user buckets).',
                      default=None)
  parser.add_argument('-debug', help='Print out log statements.', default=None, type=int)
  
  # parse the arguments
  args = parser.parse_args()
  
  exampleRequest = ExampleRequest(args.projectId, args.query, limit=args.limit, topic=args.topic,
                                  userTopic=args.userTopic, bucket=args.bucket, userBucket=args.userBucket, pathInBuckets=args.path,
                                  delim=args.delim,
                                  debug=args.debug)
  main(exampleRequest)
