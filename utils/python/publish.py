import logging
import json
import sys
from pathlib import Path

from google.cloud.pubsub_v1 import PublisherClient

logging.basicConfig(format='%(asctime)s.%(msecs)03dZ,%(pathname)s:%(lineno)d,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger(__name__)

class Publisher():
  def __init__(self, projectId=None, topic=None, columnHeadings=None, delim=None, debug=None):
    '''
    :param projectId:
    :param topic:
    :param columnHeadings: a list of column headings if the data will be provided as lists, such as from
    reading a CSV file.
    :param delim:
    :param debug:
    '''
    self._topic = topic
    self._projectId = projectId
    if topic is not None and projectId is not None: _logger.debug(
      'Output will be published to {topic} in project {projectId}.'.format(topic=topic, projectId=projectId))
    
    self._publisher = PublisherClient() if topic is not None and projectId is not None else None

    self._delim=delim
    self._columnHeadings=columnHeadings
  
  def _publish(self, data):
    '''
    Publish one message.
    :param data: a dict of key-values.
    :return: returns 1 if a message was published or 0 otherwise.
    '''
    attributes = None
    if type(data) == dict:
      attributes = {}
      for key, value in data.items():
        if type(value) in [int, float, str, bool]:
          attributes[key] = value

    if attributes is not None and len(attributes) > 0:
      # Try to include key-values of data as attributes in the published message.
      try:
        self._publisher.publish('projects/' + self._projectId + '/topics/' + self._topic,
                                data=json.dumps(data).encode("utf-8"), **attributes)
        return 1
      except:
        _logger.debug('Cannot include ' + str(attributes) + ' as attributes to the message.', exc_info=True)
    
    # Otherwise, try publishing a text message without any attributes.
    try:
      self._publisher.publish('projects/' + self._projectId + '/topics/' + self._topic,
                              data=json.dumps(data).encode("utf-8"))
      return 1
    except:
      _logger.error('Cannot publish message "' + str(json.dumps(data)) + '".', exc_info=True, stack_info=True)
      return 0
  
  def _parse(self, data):
    '''
    Parse data to generate tabular data.
    :param data: a string.
    :param delim: the delimiter used for text data; otherwise, will assume text data is comma delimited.
    :return: a dict.
    '''
    try:
      if self._columnHeadings is not None:
        parsed=dict(zip(self._columnHeadings,data.split(',' if self._delim is None else self._delim)))
      else:
        parsed=json.loads(data)
      return parsed
    except:
      _logger.error('Error parsing data as JSON string. ' + str(data), exc_info=True, stack_info=True)
      return {'error': str(data)}  # Return the record in a field named "error".
  
  def process(self, data):
    '''
    :param data:
    :return: returns (number of records written, number of records published)
    '''
    parsed = None
    try:
      _logger.debug('Received data. ' + str(data)[:100] + ('...' if len(str(data)) > 100 else ''))
      parsed = self._parse(data)
    except:
      _logger.error('Error processing data. ' + str(data), exc_info=True, stack_info=True)
    
    numPublished = 0
    if parsed is not None:
      # Output the data.
      if self._publisher is not None: numPublished += self._publish(parsed)
    return numPublished

_debugLevel=10
_columnHeadingsFile='headers.csv'
_dataFile='data.csv'

def main():
  _logger.setLevel(_debugLevel)
  
  projectId = sys.argv[1]
  topic = sys.argv[2]
  if len(sys.argv)>3:
    delim=sys.argv[3]

  if topic is not None:
    if projectId is None:
      _logger.error('Must include a project ID if you include a topic.')
      return 'Error attempting to access Pub/Sub topic with no project ID.'
    _logger.info('Will submit to {topic}.'.format(topic=topic))
  
  if Path(_columnHeadingsFile).exists():
    with Path(_columnHeadingsFile).open() as columnHeadingsContents:
      columnHeadings=columnHeadingsContents.readline().strip().split(delim if delim is not None else ',')
  else:
    columnHeadings=None
  
  publisher=Publisher(projectId=projectId,topic=topic,columnHeadings=columnHeadings,debug=_debugLevel,delim=delim)
  numPublished=0
  if Path(_dataFile).exists():
    with Path(_dataFile).open() as dataContents:
      for line in dataContents.readlines():
        numPublished+=publisher.process(line)
      _logger.info(
        'Published {numPublished:d} messages to {topic}.'.format(
          numPublished=numPublished,
          topic=topic
        ))
  else:
    _logger.error('Cannot open data file "{datafile}" to read data from.'.format(datafile=_dataFile))
  
