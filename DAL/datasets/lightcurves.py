import urllib2
import urllib
import config
import json
import pickle
from cache import Cache
from s3iterable import S3Iterable

class LightCurves(S3Iterable):
  def __init__(self,original=None):
    super(LightCurves, self).__init__() 
    self.config = config.config()
    self.bucketname = 'ml-lightcurves-q14'
    self.decompress = "unzip"
    self.parser = None

  def iter(self, subset):
    accum = ""
    for i in super(LightCurves, self).iter(subset):
      v = i.strip()
      if v == '<I1N2D3I4C5A6T7O8R9>':
        if accum.strip() != "":
          data = json.loads(accum)
          if len(data["data"]) == 9514:
            yield data
        accum = ""  
      else:
        accum += i
    if accum != '':
      data = json.loads(accum)
      accum = ""
      if len(data["data"]) == 9514:
        yield data
  
  def filter(self, subset, f):
    for i in self.iter(subset):
      if f(i):
        yield i

  def score(self, ranking):
    o = {}
    o['key'] = self.config['cache']['AWS_ACCESS_KEY']
    o['ranking'] = ranking
    data = json.dumps(o)
    req = urllib2.urlopen('http://staging.lsda.cs.uchicago.edu:8000', data)
    o = json.loads(req.read())
    return o

  def subsets(self):
    result = []
    
    for item in super(LightCurves, self).subsets():
      if "examples" not in item:
        result.append(item)
    
    return result

  def examples(self):
    return json.load(
      self.cache.directhandle(self.bucketname, "examples.json"))
  
  def example(self):
    return self.examples()
