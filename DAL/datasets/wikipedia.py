import config
from cache import Cache
from s3iterable import S3Iterable
import json

class Wikipedia(S3Iterable):
  def __init__(self):
    super(Wikipedia, self).__init__() 
    self.config = config.config()
    if config.local():
      self.bucketname = 'ml-wikipedia-local'
    else:
      self.bucketname = 'ml-wikipedia'
    self.parser = json.loads

  def byid(self, id):
    return super(Wikipedia, self).byid(
      ("chunk_{}.json".format(id // 1000), id % 1000))

  def all_articles(self):
    return xrange(9988)

  def iter(self):
    for subset in sorted(self.subsets()):
      for item in super(PNAS, self).iter(subset):
        yield item

  def test_articles(self):
    return json.load(self.cache.directhandle(self.bucketname, 'testing.id.txt'))