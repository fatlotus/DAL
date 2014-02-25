import json
import os
import os.path
import getpass

def get_config_file():
  chunks = os.path.abspath(".").split(os.path.sep)
  
  for i in xrange(len(chunks)):
    path = os.path.sep.join(chunks[:-i] + ["dalconfig.json"])
    
    if os.path.exists(path):
      return open(path, "rU")

def config():
  f = get_config_file()
  if f is not None:
    o = json.loads(f.read())
    f.close()
    return o
  else:
    raise Exception('No DAL config file detected')

def local():
  f = get_config_file()
  if f is not None:
    o = json.loads(f.read())
    f.close()
    if 'system' in o and 'local' in o['system']:
      return o['system']['local']
    return False
  else:
    raise Exception('No DAL config file detected') 
