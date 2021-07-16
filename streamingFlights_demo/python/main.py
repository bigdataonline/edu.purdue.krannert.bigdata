# The following code retrieves flight data using OpenSky's API and produces rows as JSON objects that can be
# loaded directly into a Big Query table in Google Cloud.
# This file, main.py, is set up to work with Cloud Function and can be run with an empty message.
# Alternatively you can pass in parameters to control a few aspects:
#   debug: set to 0 for no debug statements or give a debug level, otherwise assumes the level of DEBUG, which is 10.
#   bucket: Google Cloud Storage bucket for storing the data.
#   path: path within the bucket to process the data in.
#   separateLines: include a key (value doesn't matter) to create a file for each row instead of a file for all rows returned from the API call.
#
# Use the following tests from the Cloud Function UI:
#   {"message":{"projectId":"datapipe-1","separateLines":"true","topic":"flightstream","bucket":"flight_api_data","path":"test"}}

# OpenSky API is provided free of charge for non-commercial use.
# See: https://opensky-network.org/
# For the Python API, see: https://opensky-network.org/apidoc/python.html
import argparse
import json
from google.cloud import storage
import datetime

from opensky_api import OpenSkyApi

class ExampleRequest(object):
  args = None
  
  # (args.projectId,args.query,limit=args.limit,topic=args.topic,debug=args.log)
  def __init__(self, debug=None, bucket=None, path=None,separateLines=False):
    message = {'bucket': bucket,'path': path}
    if separateLines: message['separateLines']=True
    self.args = {'message': json.dumps(message)}
  
  def get_json(self):
    return json.loads(self.args['message'])

def _getMessageJSON(request):
  request_json = request.get_json(force=True)
  message = None

  if request.args is not None:
    print(json.dumps({'log': 'request is ' + str(request) + ' with args ' + str(request.args)}))
    if 'message' in request.args:
      message = request.args.get('message')

    if any(map(lambda param:param in request.args,['bucket','path','topic','projectId'])):
      # request.args holds the fields we are expecting to exist in the message.
      message = request.args
  if message is None and request_json is not None:
    # If message remains unset (None) then assuming that the request_json holds the contents of the message we are looking for.
    print(json.dumps({'log': 'request_json is ' + str(request_json)}))
    if 'message' in request_json:
      message = request_json['message']
    else:
      message = request_json
  
  if message is None:
    print('message is empty. request=' + str(request) + ' request_json=' + str(request_json))
    message = '{}'
  
  if type(message) == str:
    # If message type is str and not dict, attempt to parse as a JSON string.
    try:
      messageJSON = json.loads(message)
    except:
      try:
        print(json.dumps({'log': 'ERROR Cannot parse provided message ' + str(message)}))
      except:
        pass
      messageJSON = {}
  else:
    # Else, assuming messageJSON was decoded from a JSON object.
    messageJSON = message
  return messageJSON

class Storage(object):
  _increment = 0
  
  def _createFileName(self):
    '''
    :return: returns a unique file name to store results in.
    '''
    filename = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_' + str(self._increment)
    self._increment += 1
    return filename
  
  def __init__(self, bucket, folder=None, separateLines=False):
    self._bucket = bucket
    self._client = storage.Client().bucket(self._bucket)
    self._path = ('flightData' if folder is None else folder)
    self._separateLines = separateLines
  
  def process(self, data):
    '''
    Will write data as a series of JSON objects, one per line. NOTE that this is not a JSON list of JSON objects. Big Query will ingest the series of JSON objects on separate lines.
    :param data: a list of dicts.
    '''
    rows = map(lambda row: json.dumps(row), data)
    if self._separateLines:
      print(json.dumps({'log': 'Storing as separate files within {path}.'.format(path=self._path)}))
      for row in rows:
        fullpath=self._path + '/' + self._createFileName()
        try:
          self._client.blob(fullpath).upload_from_string(row)
        except Exception as ex:
          print(json.dumps({'log': 'ERROR Error writing to {path}: {error}'.format(path=fullpath, error=str(ex))}))
    else:
      fullpath=self._path + '/' + self._createFileName()
      print(json.dumps({'log': 'Storing in file {path}.'.format(path=self._path + '/' + self._createFileName())}))
      try:
        self._client.blob(fullpath).upload_from_string('\n'.join(rows))
      except Exception as ex:
        print(json.dumps({'log': 'ERROR Error writing to {path}: {error}'.format(path=fullpath, error=str(ex))}))

def _convertTimestamp(timestamp):
  '''
  Convert from system timestamp into a format for time that Big Query can understand.
  :param timestamp: a number
  :return: a string representation of a date and time or else None if the translation failed.
  '''
  if timestamp is not None:
    try:
      return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except:
      pass
  return None

def _convertRow(flightState, queryTime):
  '''
  :param row:
  :param queryTime:
  :return: returns a dict of the given row that can be stored as a JSON dump.
  '''
  row = {
    'icao24': flightState.icao24,
    # icao24 - ICAO24 address of the transmitter in hex string representation.
    'callsign': flightState.callsign,
    # callsign of the vehicle. Can be None if no callsign has been received.
    'origin': flightState.origin_country,  # inferred through the ICAO24 address
    'time': flightState.time_position,
    # seconds since epoch of last position report. Can be None if there was no position report received by OpenSky within 15s before.
    'contact': flightState.last_contact,
    # seconds since epoch of last received message from this transponder
    'longitude': flightState.longitude,
    # in ellipsoidal coordinates (WGS-84) and degrees. Can be None
    'latitude': flightState.latitude,
    # in ellipsoidal coordinates (WGS-84) and degrees. Can be None
    'altitude': flightState.geo_altitude,  # geometric altitude in meters. Can be None
    'on_ground': flightState.on_ground,
    # true if aircraft is on ground (sends ADS-B surface position reports).
    'velocity': flightState.velocity,
    # velocity - over ground in m/s. Can be None if information not present
    'heading': flightState.heading,
    # in decimal degrees (0 is north). Can be None if information not present.
    'vertical_rate': flightState.vertical_rate,
    # vertical_rate - in m/s, incline is positive, decline negative. Can be None if information not present.
    'sensors': flightState.sensors,
    # sensors - serial numbers of sensors which received messages from the vehicle within the validity period of this state vector. Can be None if no filtering for sensor has been requested.
    'baro_altitude': flightState.baro_altitude,
    # baro_altitude - barometric altitude in meters. Can be None
    'squawk': flightState.squawk,  # squawk - transponder code aka Squawk. Can be None
    'spi': flightState.spi,  # spi - special purpose indicator
    'position_source': flightState.position_source
    # position_source - origin of this state’s position: 0 = ADS-B, 1 = ASTERIX, 2 = MLAT, 3 = FLARM
  }
  # Following are additional fields added by this code.
  time_bq = _convertTimestamp(flightState.time_position)
  if time_bq is not None: row['time_bq'] = time_bq
  contact_bq = _convertTimestamp(flightState.last_contact)
  if contact_bq is not None: row['contact_bq'] = contact_bq
  query_time_bq = _convertTimestamp(queryTime)
  if query_time_bq is not None: row['query_time_bq'] = query_time_bq
  
  return row

def _scavengeRows(separateLines=False,bucket=None,path=None,debug=None):
  queryTime = datetime.datetime.now().timestamp()
  if debug is not None:
    print(json.dumps({'log': 'Scavenging rows at {queryTime}.'.format(queryTime=str(queryTime))}))
  api = OpenSkyApi()
  flightStates = api.get_states()
  if flightStates is not None:
    records = []
    for flightDict in map(lambda flightState: _convertRow(flightState, queryTime), flightStates.states):
      trimmedRecord = dict(filter(lambda item: item[1] is not None, flightDict.items()))
      if len(trimmedRecord) > 0:
        try:
          # If the record has at least one non-empty field, process it.
          records.append(trimmedRecord)
        except Exception as ex:
          print(json.dumps({'log': 'ERROR cannot process record due to ' + str(ex)}))
    
    if len(records) > 0:
      if debug is not None: print(json.dumps({'log': 'Found {num:d} records to process.'.format(num=len(records))}))
      # Found records to process and/or publish.
      if bucket is not None:
        storage = Storage(bucket, folder=path, separateLines=separateLines)
        storage.process(records)
        if debug is not None: print(json.dumps({
          'log': 'Stored {num:d} records in folder {path} of bucket {bucket}'.format(
            num=len(records), path=path, bucket=bucket)}))
      
  else:
    if debug is not None: print(json.dumps({'log': 'No flight records were found.'}))

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
  
  debug = messageJSON.get('debug', 10)
  if debug == 0: debug = None

  bucket = messageJSON.get('bucket', None)
  path = messageJSON.get('path', None)
  
  separateLines = 'separateLines' in messageJSON
  
  if debug is not None:
    print(json.dumps({'log': 'Parsed message is ' + json.dumps(messageJSON)}))
    if bucket is not None:
      print(json.dumps({'log':'Will store in GCS at bucket:{bucket} path:{path}'.format(bucket=bucket,path=path)}))
  _scavengeRows(separateLines=separateLines,bucket=bucket,path=path,debug=debug)

if __name__ == '__main__':
  # To call from the command line, provide two arguments: query, limit.
  parser = argparse.ArgumentParser()
  
  # add arguments to the parser
  parser.add_argument('-separateLines',help='Store each flight record as a separate file or post as a separate pub/sub entry, otherwise will process and/or post all records received from one query as a single file/entry.',default=False)

  parser.add_argument('-bucket',
                      help='Provide a GCS bucket name to process the output to if you want to persist the data in storage.',
                      default=None)
  parser.add_argument('-path',help='Specify a path within the given bucket to use for storing the data.',default=None)

  parser.add_argument('-log', help='Print out log statements.', default=None)
  
  # parse the arguments
  args = parser.parse_args()
  
  exampleRequest = ExampleRequest(debug=args.log,bucket=args.bucket,path=args.path,separateLines=args.separateLines)
  
  main(exampleRequest)
