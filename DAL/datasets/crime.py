import config
import json
try:
  import cPickle as pickle
except ImportError:
  import pickle
from cache import Cache
from s3iterable import S3Iterable
import csv
import datetime

class Crime(S3Iterable):
  def __init__(self):
    super(Crime, self).__init__() 
    self.config = config.config()
    self.bucketname = "ml-crime"
    self.decompress = "unzip"
    self.parser = json.loads
  
  def get_crime_list(self):
    """
    Returns all crime types.
    """

    fp = self.cache.directhandle(self.bucketname, "type_file.zip",
           decompress="unzip")
    return pickle.load(fp)

  def get_crime_counts(self):
    """
    Returns the numbers of each type of crime.
    """

    fp = self.cache.directhandle(self.bucketname, "count_file.zip",
           decompress="unzip")
    return pickle.load(fp)

  def get_region_list(self):
    """
    Iterates over all regions in the dataset.
    """

    fp = self.cache.directhandle(self.bucketname, "center_file.zip",
           decompress="unzip")
    return pickle.load(fp)

  def iter(self):
    """
    Iterates over all crimes in the dataset.
    """

    fp = self.cache.directhandle(self.bucketname, "raw_file.zip",
           decompress="unzip")

    for row in csv.reader(fp):
      try:
        date, lat, lon, crime_type = row
        
        day = datetime.datetime.strptime(date, "%m/%d/%Y %I:%M:%S %p")
        latitude = float(lat)
        longitude = float(lon)
        
        yield day, latitude, longitude, crime_type
      except ValueError:
        pass

  """
  def metadata(self):
    #dfnk-7re6.json.meta.json => set0
    #ij.source.meta.meta.json => set1
    df = self.cache.directhandle(self.bucketname, 'set0.meta.json', decompress=None)
    ij = self.cache.directhandle(self.bucketname, 'set1.meta.json', decompress=None)
    o = {}
    o['set0.meta.json'] = json.loads(df.read())
    o['set1.meta.json'] = json.loads(ij.read())
    return o

  def get_crime_list(self):
    f = self.cache.directhandle(self.bucketname, 'crime_list.zip', decompress="unzip")
    return pickle.load(f)

  def get_crime_counts(self):
    f = self.cache.directhandle(self.bucketname, 'crime_counts.zip', decompress="unzip")
    return pickle.load(f)
 
  def get_region_list(self):
    f = self.cache.directhandle(self.bucketname, 'region_list.zip', decompress="unzip")
    return pickle.load(f)
"""
 
    
