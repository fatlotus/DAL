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
import mimetypes
import time

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

        kind, reference = state

        if kind == "s3":
            bucket = boto.connect_s3().get_bucket("ml-checkpoints",
                       validate = False)

            if bucket.get_key(reference) is None:
                raise ValueError("FileReference no longer exists.")

        elif kind == "local":
            if not os.path.isfile(reference):
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

def attach_file(file_or_file_name):
    """
    Outputs a local file in a persistent S3 storage space.
    """

    from IPython.core.display import HTML, display
    
    # Compute the path of the given file.
    if hasattr(file_or_file_name, 'name'):
        file_name = file_or_file_name.name
    else:
        file_name = file_or_file_name
    
    if not os.path.exists(file_name):
        raise ValueError("The file {} does not exist.".format(file_name))
    
    if running_on_aws():
        import boto
        
        # Prepare basic key in upload.
        upload_key = str(uuid.uuid4())
        
        bucket = boto.connect_s3().get_bucket("ml-submissions")
        key = bucket.new_key("attachments/" + upload_key)
        
        # Attempt to guess the MIME-type of this file.
        kind, _ = mimetypes.guess_type(file_name)
        key.content_type = kind or 'application/octet-stream'
        
        # Upload the given file to S3.
        key.set_contents_from_filename(file_name)
        
        # Save the public URL of this file.
        public_url = (
            "http://ml-submissions.s3-website-us-east-1.amazonaws.com/"
            "attachments/" + upload_key
        )
    
    else:
        # Store a local link to the file.
        public_url = "file://" + os.path.abspath(file_name)
    
    # Compute the display name of the file.
    display_name = os.path.basename(file_name)
    size = os.path.getsize(file_name)
    
    # Format the size with a unit.
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    while size >= 1024:
        size /= 1024
        units.pop(0)
    unit = units[0]
    
    display(HTML("""
    <div style="background-color:#eee;border:1px solid #ccc;padding:0.5em;">
        Attachment: <a href="{public_url}">{display_name}</a> ({size} {unit})
    </div>
    """.format(**locals())))

def shutdown_task():
    """
    Halts the execution of this task.
    """

    sys.__stdout__.write("DEQUEUE_THIS_TASK")
    sys.__stdout__.flush()

def request_size(requested_size, timeout = 30):
    """
    Requests that the cluster be resized to the given size.
    """

    # Do not allow local resizes.
    if not running_on_aws():
        return

    # Get the current state of the cluter.
    from IPython.parallel import Client
    rc = Client()

    # Validate the requested size.
    size = requested_size

    if size < 0 or type(size) is not int:
        raise ValueError("Invalid worker count {!r}".format(size))

    # Instances come in twos, and there must be at least one.
    size = (size // 2) * 2

    # Fix proper instance size.
    if size < 2:
        size = 2

    # Limit the total size of the cluster, for now.
    if size > 20:
        size = 20

    # Count the number of machines to be added or removed.
    machine_count = (len(rc) - size) // 2

    # If we need to remove nodes, do so.
    if machine_count > 0:
        rc[-machine_count *2::2].apply_sync(shutdown_instance)

    # Otherwise, add nodes.
    elif machine_count < 0:
        for i in xrange(-machine_count):
            sys.__stdout__.write("SPAWN_NEW_COPY")
        sys.__stdout__.flush()

    # Wait until everything is ready.
    while len(rc) != size and timeout > 0:
        time.sleep(1)
        timeout -= 1

    # Warn the user if the operation failed.
    if len(rc) != requested_size:
        print("Warning: requested {} instances, but only got {}.".format(
            requested_size, len(rc)
        ))
