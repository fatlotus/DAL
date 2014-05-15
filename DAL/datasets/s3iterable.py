import config
from cache import Cache

class S3Iterable(object):
  def __init__(self):
    '''
    Subclasses must handle setting up config including:
    * bucketname
    * parser
    '''
    self.bucketname = None
    self.parser = None
    self.cache = Cache()
    self.iterator = iter
    self.decompress = None

  def subsets(self):
    return [i.key for i in self.cache.s3listcontents(self.bucketname)
            if not i.key.endswith("meta.txt") ]

  def iter(self, subset):
    h = self.cache.directhandle(self.bucketname, subset, decompress=self.decompress)
    for l in self.iterator(h):
      if not l.strip(): # mask blank lines
        continue
      
      if self.parser is None:
        yield l
      else:
        try:
          parsed = self.parser(l)
        except:
          pass
        else:
          yield parsed

  def filter(self, subset, f):
    h = self.cache.directhandle(self.bucketname, subset, decompress=self.decompress)
    for l in self.iterator(h):
      if self.parser is None:
        j = l
      else:
        try:
          j = self.parser(l)
        except:
          continue
      if f(j):
        yield j

  def byid(self, index):
    (subset, i) = index
    h = self.cache.directhandle(self.bucketname, subset, decompress=self.decompress)
    c = 0
    for l in self.iterator(h):
      if not l.strip(): # mask blank lines
        continue

      if self.parser:
        try:
          parsed = self.parser(l)
        except:
          continue
      else:
        parsed = l
      
      if c == i:
        return parsed
      else:
        c += 1
    return None 

  def display(self, items):
    for i in items:
      print i
 
