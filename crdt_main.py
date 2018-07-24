import collections

class LWWElement(dict, collections.MutableMapping):
    def __setitem__(self, payload, timestamp):
        if not isinstance(payload, collections.Hashable):
            raise TypeError('unhashable type {}'.format(type(payload)))
        if not isinstance(timestamp, int):
            raise TypeError('timestamp expected to be an epoch time expressed in 32 bit integer')
        super(LWWElement, self).__setitem__(payload, timestamp)

class LWWElementSet(object):
    """Conflict Free Replicated Data Type (CRDT) implemented 
        using Last Writer Wins (LWW) algorythm.
    """
    def __init__(self):
        self._set_add = LWWElement()
        self._set_remove = LWWElement()

    def add(self, payload, timestamp):
        self._set_add[payload] = timestamp

    def remove(self, payload, timestamp):
        self._set_remove[payload] = timestamp
