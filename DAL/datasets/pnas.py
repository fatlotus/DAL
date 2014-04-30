import config
from cache import Cache
from s3iterable import S3Iterable
import json

class PNAS(S3Iterable):
  def __init__(self):
    super(PNAS, self).__init__() 
    self.config = config.config()
    if config.local():
      self.bucketname = 'ml-pnas-local'
    else:
      self.bucketname = 'ml-pnas'
    self.parser = json.loads

  def byid(self, id):
    return super(PNAS, self).byid(
      ("chunk_{}.json".format(id // 1000), id % 1000))

  def article_ids(self, id):
    return xrange(13948)