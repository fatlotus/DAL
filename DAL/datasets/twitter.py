import config
from cache import Cache
from s3iterable import S3Iterable
import json

class Twitter(S3Iterable):
  def __init__(self):
    super(Twitter, self).__init__() 
    self.config = config.config()
    if config.local():
      self.bucketname = self.config['twitter']['bucket']+'-local'
    else:
      self.bucketname = self.config['twitter']['bucket']

    self.iterator = self.timed_iterator
    self.parser = json.loads

    self.prediction = None

  def iter(self):
    """
    Iterates over all subsets of this dataset.
    """

    for subset in sorted(self.subsets()):
      super(Twitter, self).iter(subset)

  def timed_iterator(self, x):
    """
    Iterates over the given dataset, ensuring that the students read the
    tweets fast enough.
    """

    start = time.time()
    for line in x:
      skipped = 0
      while time.time() - start < offset:
        skipped += 1
      if skipped != 0:
        print("WARNING: Your code is too slow, and so missed {} tweets.".
          format(skipped))
      offset, data = line.split(",", 1)
      yield data

  def byid(self, x):
    """
    No-op.
    """

    raise Exception("Tweets can only be streamed.")