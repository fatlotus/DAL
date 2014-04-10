import config
from cache import Cache
from s3iterable import S3Iterable

class Digits(S3Iterable):
  def __init__(self):
    super(Digits, self).__init__() 
    self.config = config.config()
    if config.local():
      self.bucketname = self.config['digits']['bucket']+'-local'
    else:
      self.bucketname = self.config['digits']['bucket']

  def subsets(self):
    """
    Returns a list of all digits.
    """

    if self.config().local():
      return [3, 5]
    else:
      return range(10)

  def iter(self, subset, digit):
    """
    Generate data for the given digit. The value +subset+ should be either
    "train" or "test", as appropriate.
    """

    lines = super(Digits, self).iter("{}{}.csv".format(subset, digit))

    for item in lines:
      parts = [float(x) for x in item.split(",")]
      yield (numpy.array(parts[:-1], dtype=numpy.float), float(parts[-1]))
