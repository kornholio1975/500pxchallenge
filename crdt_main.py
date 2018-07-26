import collections

class LWWEInset(dict, collections.MutableMapping):
    """Dict subclass for storing LWW Elements in CRDT LWWElementSet.

    Stores a key value pair where::
        key: any serializble type storing a payload
        value: epoch time expressed as 32bit integer representing a timestamp
    """
    def __setitem__(self, payload, timestamp):
        #Override dict __setitem__ to enforce correct data types
        if not isinstance(payload, collections.Hashable):
            raise TypeError('unhashable type {}'.format(type(payload)))
        if not isinstance(timestamp, int):
            raise TypeError('timestamp expected to be an epoch time expressed in 32 bit integer')
        super(LWWEInset, self).__setitem__(payload, timestamp)

class LWWElementSet(object):
    """Conflict Free Replicated Data Type (CRDT) implemented using Last Writer Wins (LWW) algorythm.

    Usage::
        >>> import crdt_main
        >>> data_set = crdt_main.LWWElementSet()
        >>> data_set.add('foo', 1532565895)
        >>> data_set.remove('foo', 1532565941)
    """

    def __init__(self):
        self._set_add = LWWEInset()
        self._set_remove = LWWEInset()

    def add(self, payload, timestamp):
        self._set_add[payload] = timestamp

    def remove(self, payload, timestamp):
        self._set_remove[payload] = timestamp
