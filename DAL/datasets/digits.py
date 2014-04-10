import config
from cache import Cache
from s3iterable import S3Iterable
import numpy

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

    if config.local():
      return [3, 5]
    else:
      return range(10)

  def iter(self, subset, digit):
    """
    Generate data for the given digit. The value +subset+ should be either
    "train" or "test", as appropriate.
    """

    if subset not in ("train", "test"):
      raise ValueError("Unknown subset {!r}".format(subset))
    
    if digit not in self.subsets():
      raise ValueError("Unknown digit {!r} (must be one of {!r})".format(
        digit, self.subsets()
      ))

    lines = super(Digits, self).iter("{}{}.csv".format(subset, digit))

    for item in lines:
      parts = [float(x) for x in item.split(",")]
      yield (numpy.array(parts[:-1], dtype=numpy.float), float(parts[-1]))
