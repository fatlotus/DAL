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
import sys, json
from datasets import config
import uuid

__all__ = ["sort_file", "set_flag"]

Keyed = namedtuple("Keyed", ["key", "obj"])


def merge(key=None, *iterables):
    """
    Merges the list of iterables by the given key function.
    """

    if key is None:
        keyed_iterables = iterables
    else:
        keyed_iterables = [(Keyed(key(obj), obj) for obj in iterable)
                           for iterable in iterables]

    for element in heapq.merge(*keyed_iterables):
        yield element.obj


def sort_file(input, output, key=None, buffer_size=1000000, tempdirs=None):
    """
    Sorts the given input file, writing the output to the given output file.
    
    This function uses +key+, a function, to compute the sort field of each
    record, and +tempdirs+, a list of paths, to determine where to store data.
    """

    if tempdirs is None:
        tempdirs = []

    if not tempdirs:
        tempdirs.append(tempfile.gettempdir())

    chunks = []
    try:
        with open(input, 'rb', 64 * 1024) as input_file:
            input_iterator = iter(input_file)

            for tempdir in cycle(tempdirs):
                current_chunk = list(islice(input_iterator, buffer_size))
                if not current_chunk:
                    break

                current_chunk.sort(key=key)
                output_chunk = open(os.path.join(
                    tempdir, '{:06d}'.format(len(chunks))), 'w+b', 64 * 1024)
                chunks.append(output_chunk)
                output_chunk.writelines(current_chunk)
                output_chunk.flush()
                output_chunk.seek(0)

        with open(output, 'wb', 64 * 1024) as output_file:
            output_file.writelines(merge(key, *chunks))

    finally:
        for chunk in chunks:
            try:
                chunk.close()
                os.remove(chunk.name)
            except Exception:
                pass

def merge_sorted_files(inputs, ouputs, key=None):
    """
    Merges the given presorted input files into a single sorted output file.
    
    This function uses +key+, a function, to compute the sort field of each
    record, and +tempdirs+, a list of paths, to determine where to store data.
    """

    with open(output, 'wb', 64 * 1024) as output_file:
        output_file.writelines(merge(key, *chunks))

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

    def __init__(self, file_or_name):
        """
        Initialize this FileReference with the given file.
        """

        if isinstance(file_or_name, file):
            self._filename = file_or_name.name
        else:
            self._filename = file_or_name
        self._location = None

    def __getstate__(self):
        """
        Returns a serializable reference for this object.
        """

        if self._location:
            return self._location

        if running_on_aws():
            location = uuid.uuid4()

            bucket = boto.connect_s3().get_bucket("ml-checkpoints")
            bucket.new_key(location).put_contents_from_filename(self._filename)

            self._location = ("s3", location)
        else:
            self._location = ("local", self._filename)

        return self._location

    def __setstate__(self, state):
        """
        Unpickles this object with the given state dictionary.
        """

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
            self._filename = tempfile.mkstemp()

            bucket = boto.connect_s3().get_bucket("ml-checkpoints")
            bucket.new_key(refernece).get_contents_to_filename(self._filename)

        return self._filename

    def open(self, mode="r"):
        """
        Returns a file handle to this FileReference.
        """

        return open(self.filename, mode)