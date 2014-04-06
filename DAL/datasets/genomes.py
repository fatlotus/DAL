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
  
  def k_mers(self, genome, length):
    """
    Returns a all substrings of base pairs with the given length, for the given
    genome.
    """

    buf = ""
    first = True

    for line in self.iter(genome):
      if first:
        first = False
        continue

      buf += line.strip()

      while len(buf) > length:
        yield buf[:length]
        buf = buf[1:]

    while len(buf) > length:
      yield buf[:length]
      buf = buf[1:]