import config
import json
from cache import Cache
from s3iterable import S3Iterable

class WishesLabelled(S3Iterable):
  def __init__(self):
    super(WishesLabelled, self).__init__() 
    self.config = config.config()
    if config.local():
      self.bucketname = 'ml-wishes-labelled-local' 
    else:
      self.bucketname = 'ml-wishes-labelled'
    self.parser = json.loads
  
  def subsets(self):
    """
    Returns the subsets of the labelled wishes dataset.
    """
    
    result = []
    
    for item in super(WishesLabelled, self).subsets():
      if item != "oracle.json":
        result.append(item)
    
    return result
  
  def eval(self, values):
    """
    Returns the fraction of Tweets that were classified correctly, given
    a dictionary mapping from +tweet_id+'s to classes.
    """
    
    if config.local():
      raise ValueError("Can only evaluate performance on the cluster.")
    
    oracle = json.load(self.cache.directhandle(self.bucketname, "oracle.json"))
    
    correct = 0
    total = 0
    
    for key, value in oracle.items():
      if oracle[key] == values.get(key, None):
        correct += 1
      total += 1
    
    return float(correct) / total