# helpers.py
#
# Contains basic helper functions for use in DAL-aware applications.
#
# Contributors:
#
# Sort algorithm:
# - Python 2.6 version: http://code.activestate.com/recipes/576755/
# - Original: http://code.activestate.com/recipes/466302/
#
# Merge function:
# - http://groups.google.com/group/comp.lang.python/msg/484f01f1ea3c832d
#

"""
This file contains basic helper functions for use in DAL-aware applications,
including a disk-backed merge sort algorithm.
"""

import os
import tempfile
from itertools import islice, cycle
from collections import namedtuple
import heapq
import functools
import sys, json
from datasets import config
import hashlib
import uuid
import boto
import marshal
import pickle

def set_status(message):
    """
    Sets the status of this task, visible in the control panel.
    """

    sys.__stdout__.write("REPORTING_SEMAPHORE {}\n".format(json.dumps(message)))
    sys.__stdout__.flush()

def running_on_aws():
    """
    Returns true if we are running on AWS.
    """

    return not config.local()

class FileReference(object):
    """
    Represents a serializable reference to an object.
    """

    def __init__(self, file_or_name, keep_local_copy = True):
        """
        Initialize this FileReference with the given file.
        """

        if isinstance(file_or_name, file):
            self._filename = file_or_name.name
        else:
            self._filename = file_or_name
        self._location = None

        if keep_local_copy is False:
            self.remove_local_copy()

    def __getstate__(self):
        """
        Returns a serializable reference for this object.
        """

        if self._location:
            return self._location

        if running_on_aws():
            location = "blobs/{}".format(uuid.uuid4())

            bucket = boto.connect_s3().get_bucket("ml-checkpoints",
                       validate = False)
            bucket.new_key(location).set_contents_from_filename(self._filename)

            self._location = ("s3", location)
        else:
            self._location = ("local", self._filename)

        return self._location

    def __setstate__(self, state):
        """
        Unpickles this object with the given state dictionary.
        """

        bucket = boto.connect_s3().get_bucket("ml-checkpoints",
                   validate = False)
        if bucket.get_key(location) is None:
            raise ValueError("FileReference no longer exists.")

        self._location = state
        self._filename = None

    @property
    def filename(self):
        """
        Returns a local filename for this FileReference.
        """

        if self._filename:
            return self._filename

        type, reference = self._location

        if type == "local":
            self._filename = reference
        else:
            self._filename = tempfile.mkstemp()[1]

            bucket = boto.connect_s3().get_bucket("ml-checkpoints")
            bucket.new_key(reference).get_contents_to_filename(self._filename)

        return self._filename

    def remove_local_copy(self):
        """
        Ensures that this FileReference does not count towards the quota.
        """

        if not self._location:
            self.__getstate__()

        os.remove(self._filename)

    def open(self, mode="r"):
        """
        Returns a file handle to this FileReference.
        """

        return open(self.filename, mode)

def only_once(function):
    """
    Ensures that the given function is only run once with the given input.
    """

    # Only enable memoization on Amazon Web Services.
    if not running_on_aws():
        return function

    # Connect to the proper bucket.
    bucket = boto.connect_s3().get_bucket("ml-checkpoints")

    # Prepare per-user memoization prefix for this function.
    prefix = hashlib.sha1("{}:{}".format(
      os.environ.get('SUBMITTER', '').encode('utf-8'),
      marshal.dumps(function.func_code)
    )).hexdigest()

    @functools.wraps(function)
    def inner(*vargs, **dargs):
        """
        Wrapper for the function to memoize.
        """

        # Set up memoization.
        key_name = "checkpoints/{}".format(
          hashlib.sha1(pickle.dumps((vargs, dargs))).hexdigest())
        key = bucket.get_key(key_name)
        result = None

        # Look up and parse the relevant key.
        if key:
            packed_data = key.get_contents_as_string()

            try:
                result = pickle.loads(packed_data)
            except Exception:
                key = None

        # Evaluate the function, if we need to.
        if not key:
            result = function(*vargs, **dargs)
            key = bucket.new_key(key_name)

        # Save the result before returning.
        bucket.new_key(key_name).set_contents_from_string(
          pickle.dumps(result))
        return result

    return inner
