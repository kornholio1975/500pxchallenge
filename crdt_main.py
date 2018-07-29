# -*- coding: utf-8 -*-
""" Conflict Free Replicated Data Type (CRDT) implemented using Last Writer Wins (LWW) algorythm.
    Python built in types used only.
"""

import collections

class LWWEInset(dict, collections.MutableMapping):
    """Dict subclass for storing LWW Elements in CRDT LWWElementSet.

    Stores a key value pair where::
        key: any serializble type storing a payload
        value: epoch time expressed as float representing a timestamp
    """
    def __setitem__(self, payload, timestamp):
        #Override dict __setitem__ to enforce correct data types
        if not isinstance(payload, collections.Hashable):
            raise TypeError('unhashable type {}'.format(type(payload)))
        if not isinstance(timestamp, float):
            raise TypeError('timestamp expected to be an epoch time expressed as float')
        super(LWWEInset, self).__setitem__(payload, timestamp)

class LWWElementSet(object):
    """Conflict Free Replicated Data Type (CRDT) implemented using Last Writer Wins (LWW) algorythm.

    Usage::
        >>> import crdt_main
        >>> data_set = crdt_main.LWWElementSet()
        >>> data_set.add('foo', 1532565895.0)
        >>> data_set.exists('foo')
        >>> data_set.get()
        >>> data_set.remove('foo', 1532565941.0)
    """

    def __init__(self):
        self._set_add = LWWEInset()
        self._set_remove = LWWEInset()

    def add(self, payload, timestamp):
        # Add element to the set
        self._set_add[payload] = timestamp

    def remove(self, payload, timestamp):
        # Remove element from the set
        self._set_remove[payload] = timestamp

    def exists(self, payload):
        # Test if element still exists
        timestamp_added = self._set_add.get(payload)
        timestamp_removed = self._set_remove.get(payload)
        return timestamp_added > timestamp_removed

    def get(self):
        # Return an array of most recent elements
        return [payload for payload in self._set_add.iterkeys() if self.exists(payload)]
