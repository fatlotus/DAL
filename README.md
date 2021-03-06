Data Access Layer (DAL) for lsdacluster
=======================================

The data access layer (DAl) makes working with lsdacluster's large datasets much easier.  It does this by transparently exposing the datasets with a relatively uniform API.

Example usage
=============

```python
from DAL import create
#create a handle to the tinyimages dataset
tinyimages = create('tinyimages')

#load in tinyimages 0 through 99
x = tinyimages.byid((0, 100))

#display those images
tinyimages.display(x)
```

See examples/test.py to see a sample usage of each of the available datasets.

Data set handles
================

A handle to a dataset is returned by called the create method with the name of the dataset as the parameter.

```python
def create(name)
```

Currently supported datasets:

* Tiny Images ('tinyimages')
* Wishes ('wishes')
* State of the Union ('sou')
* Google Ngrams Version 1. ('ngrams')

Uniform Api
===========

Each dataset must have three methods: byid and display.

```python
def byid (index)

def display(array)

def subsets()
```

byid takes in an index and returns the associated data items.  Each dataset defines how it is indexed. 

display takes in an array of dataitems and displays them in an ipython notebook.

subsets returns a listing of the names of the subsets of a dataset or None when a dataset has known names subsets.

Tiny Images - Dataset Specific API (DSA)
========================================

Tiny images are indexed by numeric ids.  byid takes in an index (integer), a (start, end) pair, or an array of indices.

Tiny images features a search command that takes in a keyword and limit and will return up to limit image indices associated with the keyword.

```python
def search(keyword, limit)
```

Wishes - DSA
============

The index of a wish is a pair: (subsetname, numeric_id).  Byid takes in a pair: (subsetname, numeric_id).  

Using the identifiers from a call to subsets, you can call iter on a subset of the wishes.  Iter returns an iterator that allows you to iterate over the entire subset.

```python
def iter(subset)
```

Filter is just like iter except it also takes in a function, f, that used to filter the items returned by the iterator.  Only items that return true when passed into f will be returned.

```python
def filter(subset, f)
```

State of Union - DSA
====================

Identical to Wishes.

Google Ngrams - DSA
===================

Identical to Wishes.


checkpoint
==========

A key/val store for checkpointing python objects to AWS S3.  The current serialization is pickle.  I'm working on improving everything.

I built this for saving the output of long running python scripts for later use. 

Look how simple:

```python
from DAL.datasets.checkpoint import Checkpoint

ch = Checkpoint()

x = [1,2,3,4,5,6, 7]

ch.store('mylist1', x)

y = ch.load('mylist1')

print y

l = ch.list()
for i in l:
  print i.key

```

Setup
=====

Since this is built on top of boto, you should have these two set:
* AWS_ACCESS_KEY_ID 
* AWS_SECRET_ACCESS_KEY 
 
Optionally, you can set your s3 bucket in the environment variable, CHECKPOINT_BUCKET. 

Checkpoint
==========

Our main class, Checkpoint, exposes a simple API to the user that allows them to load and store key/value pairs into s3.  Objects are serialized into s3 via store and deserialized back via load.

Functions
=========

Checkpoint.store(key, obj)
==========================

Stores obj at key in your bucket.  

e.g.

```python
ch.store('mylist1', x)
```

Checkpoint.load(key)
====================

Returns the object stored at key in your bucket.

e.g.
```python
y = ch.load('mylist1')
```

Checkpoint.list()
=================

Returns at interator over the items in your bucket.

e.g.
```python
l = ch.list()
for i in l:
  print i.key
```
