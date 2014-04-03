import config
from cache import Cache
from s3iterable import S3Iterable

class Genomes(S3Iterable):
  def __init__(self):
    super(Genomes, self).__init__() 
    self.config = config.config()
    if config.local():
      self.bucketname = self.config['genomes']['bucket']+'-local'
    else:
      self.bucketname = self.config['genomes']['bucket']