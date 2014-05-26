import config
import pickle 
import os
import os.path
import subprocess
import fcntl
import time
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
import czipfile as zipfile
from collections import defaultdict
import os
import errno
from httplib import IncompleteRead
import tempfile
import shutil

#TODO - size handling
def parseSize(s):
  pass

def decompress_name(name):
  pieces = name.split('.')
  return '.'.join(pieces[:(len(pieces) - 1)])

def storage_name(path, name, bucketname):
  return path+'/' + bucketname+'-'.join(name.split('/'))
 
class Cache:
  def __init__(self):
    self.config = config.config()
    self.path = self.config['cache']['path']
    self.size = parseSize(self.config['cache']['size'])
    
    try:
      os.makedirs(self.path)
    except OSError as exc: # http://stackoverflow.com/a/600612/1028526
      if exc.errno == errno.EEXIST and os.path.isdir(self.path):
        pass
      else:
        raise

  def connect(self):
    return S3Connection(
      aws_access_key_id = self.config.get('aws_access_key_id'),
      aws_secret_access_key = self.config.get('aws_secret_access_key'),
      calling_format = OrdinaryCallingFormat(),
    )

  def s3listcontents(self, bucketname):
    conn = self.connect()
    b = conn.get_bucket(bucketname)
    o = b.list()
    conn.close()
    return o
  
  def s3tocache(self, bucketname, objname, decompress=None):
    conn = self.connect()
    b = conn.get_bucket(bucketname)
    k = Key(b)
    k.key = objname
    path = storage_name(self.path, objname, bucketname)
    
    for i in xrange(5):
        try:
            k.get_contents_to_filename(path)
        except IncompleteRead:
            k.close(fast = True)
            time.sleep(1)
        else:
            break
    else:
        k.get_contents_to_filename(path)
    
    if decompress is not None:
      self.decompress(decompress, path) 
       
  def decompress(self, algorithm, zip_file_path):
    """
    Uses the given algorithm to decompress +path+ to
    +decompress_name(path)+.
    """
    
    resulting_path = decompress_name(zip_file_path)
    
    if not os.path.isfile(resulting_path):
      if algorithm == 'unzip':
        extraction_directory = self.path + "/" + str(time.time())
        
        try:
          archive = zipfile.ZipFile(zip_file_path)
          archive.extractall(path=extraction_directory)
          
          contents = archive.namelist()[0]
          os.rename(os.path.join(extraction_directory, contents),
            resulting_path)
          
        finally:
          try:
            shutil.rmtree(extraction_directory)
          except:
            pass
        
      else:
        raise ValueError("Unknown decompression algorithm: {!r}".
           format(algorithm))
    
    return resulting_path
  
  def cleancache(self):
    return None
  
  def directhandle(self, bucketname, objname, decompress=None, binary=None):
    if decompress is None:
      path = storage_name(self.path, objname, bucketname)
    else:
      path = decompress_name(storage_name(self.path, objname, bucketname))
    if os.path.isfile(path):
      if binary is not None:
        return open(path, 'rb')
      else:
        return open(path)
    else:
      self.s3tocache(bucketname, objname, decompress=decompress)
      if binary is not None:
        return open(path, 'rb')
      else:
        return open(path)
  
  def __getStateFromLog(self, bucketname, objname):
    filename = bucketname + "-" + objname
    if os.path.exists(os.path.join(self.path, filename)):
      return "COMPLETE"
    else:
      return NONE
   
  def __lockOrNone(self, bucketname, obj):
    return None

  def __logWithLock(self, bucketname, objname, state):
    pass

  def __log(self, bucketname, objname, state):
    entrylog = open(self.path+'/cache.log', "a+") 
    entry = "%s %s %s" % (bucketname, objname, state)
    entrylog.write(entry+'\n')   
    entrylog.close()
