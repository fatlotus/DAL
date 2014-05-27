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
      if "part" in item:
        result.append(item)
    
    return result

  def examples(self):
    return json.load(
      self.cache.directhandle(self.bucketname, "examples.json"))
  
  def example(self):
    return self.examples()

  def prcurve_a(self, ranking):
      """
      Display the precision-recall curve for part A.
      """
      
      if config.local():
          return
      
      import numpy as np
      import matplotlib.pyplot as plt
      import pickle
      
      # Change strings into numbers for IDs. 
      mapping = pickle.load(self.cache.directhandle(self.bucketname,
                             "mapp.pickle"))
      as_dict = dict([(y, x) for (x, y) in mapping])
      ranking = [as_dict.get(x, None) for x in ranking]
      as_dict = None
      mapping = None
      
      # Retrieve IDs from DAL.
      conf_cand_eb_id = [int(i) for i in
                             self.cache.directhandle(self.bucketname, 'conf_cand_eb_id.txt')]
      conf_id = [int(i) for i in self.cache.directhandle(self.bucketname, 'conf_id.txt')]
      conf_and_cand_id = [int(i) for i in
                          self.cache.directhandle(self.bucketname, 'conf_and_cand_id.txt')]
      conf_and_eb_id = [int(i) for i in
                        self.cache.directhandle(self.bucketname, 'conf_and_eb_id.txt')]
      
      # conf + eb
      precision = []
      recall = []
      count = 0.0
      N = len(conf_and_eb_id)
      for i in xrange(len(ranking)):
          ID = ranking[i]
          if ID in conf_and_eb_id:
              count += 1.0
              precision.append( count/(i+1) )
              recall.append( count/ N )
          if count == N:
              break
            
      a = np.array(precision, dtype=np.float32)
      area = np.sum(a / N)
    
      fig = plt.figure(figsize=(10,12))
      ax = fig.add_subplot(2,1,1)
      ax.plot(recall, precision, '-r',linewidth=2)
      ax.set_xlabel('recall', fontsize=15)
      ax.set_ylabel('precision',fontsize=15)
      ax.set_xlim((-0.01,1.01))
      # ax.set_title('CONF+EB area = {0}'.format(area), fontsize=15)
      plt.grid(True)
    
      # conf + eb + cand
      precision = []
      recall = []
      count = 0.0
      N = len(conf_cand_eb_id)
      for i in xrange(len(ranking)):
          ID = ranking[i]
          if ID in conf_cand_eb_id:
              count += 1.0
              precision.append(count/(i+1))
              recall.append(count/N)
          if count == N:
              break
            
      b = np.array(precision, dtype=np.float32)
      area_1 = np.sum(b/N)
    
      ax = fig.add_subplot(2,1,2)
      ax.plot(recall, precision, '-r',linewidth=2)
      ax.set_xlabel('recall', fontsize=15)
      ax.set_ylabel('precision',fontsize=15)
      ax.set_xlim((-0.01,1.01))
      # ax.set_title('CONF+CAND+EB area = {0}'.format(area_1), fontsize=15)
      plt.grid(True)
      plt.show()
    
      return (area, area_1)

  def prcurve_b(self, ranking):
      """
      Display the precision-recall curve for part B.
      """
      
      if config.local():
          return
      
      import numpy as np
      import matplotlib.pyplot as plt
      import pickle
      
      # Change strings into numbers for IDs. 
      mapping = pickle.load(self.cache.directhandle(self.bucketname,
                             "mapp.pickle"))
      as_dict = dict([(y, x) for (x, y) in mapping])
      ranking = [as_dict.get(x, None) for x in ranking]
      as_dict = None
      mapping = None
      
      # Retrieve IDs from DAL.
      conf_cand_eb_id = [int(i) for i in
                             self.cache.directhandle(self.bucketname, 'conf_cand_eb_id.txt')]
      conf_id = [int(i) for i in self.cache.directhandle(self.bucketname, 'conf_id.txt')]
      conf_and_cand_id = [int(i) for i in
                          self.cache.directhandle(self.bucketname, 'conf_and_cand_id.txt')]
      conf_and_eb_id = [int(i) for i in
                        self.cache.directhandle(self.bucketname, 'conf_and_eb_id.txt')]
    
      # conf vs eb
      order = [i for i in ranking if i in conf_and_eb_id]
      precision = []
      recall = []
      count = 0.0
      i = 0.0
      N = len(conf_id)
      for ID in order:
          i += 1.0
          if ID in conf_id:
              count += 1.0
              precision.append( count/ i )
              recall.append( count / N )
        
              if count == N:
                  break

      area = np.sum(np.array(precision, dtype=np.float32) / N)
    
      fig = plt.figure(figsize=(10,12))
      ax = fig.add_subplot(2,1,1)
      ax.plot(recall, precision, '-r', linewidth=2)
      ax.set_xlabel('recall',fontsize=15)
      ax.set_ylabel('precision',fontsize=15)
      ax.set_title('conf vs eb area = {0}'.format(area),fontsize=15)
      plt.grid(True)


      order = [i for i in ranking if i in conf_cand_eb_id]
      precision = []
      recall = []
      count = 0.0
      i = 0.0
      N = len(conf_and_cand_id)
      for ID in order:
          i += 1.0
          if ID in conf_and_cand_id:
              count += 1.0
              precision.append( count/ i )
              recall.append( count / N )
        
              if count == N:
                  break
    
      area_1 = np.sum(np.array(precision, dtype=np.float32) / N)
    
      ax = fig.add_subplot(2,1,2)
      ax.plot(recall, precision, '-r', linewidth=2)
      ax.set_xlabel('recall',fontsize=15)
      ax.set_ylabel('precision',fontsize=15)
      ax.set_title('conf+cand vs eb area = {0}'.format(area_1),fontsize=15)
      plt.grid(True)
      plt.show()
    
      return (area, area_1)