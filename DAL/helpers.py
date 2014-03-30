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
from tempfile import gettempdir
from itertools import islice, cycle
from collections import namedtuple
import heapq
import sys, json

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


def sort_file(input, output, key=None, buffer_size=32000, tempdirs=None):
    """
    Sorts the given input file, writing the output to the given output file.
    
    This function uses +key+, a function, to compute the sort field of each
    record, and +tempdirs+, a list of paths, to determine where to store data.
    """

    if tempdirs is None:
        tempdirs = []

    if not tempdirs:
        tempdirs.append(gettempdir())

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

def set_status(message):
    """
    Sets the status of this task, visible in the control panel.
    """

    sys.__stdout__.write("REPORTING_SEMAPHORE {}\n".format(json.dumps(message)))
    sys.__stdout__.flush()
