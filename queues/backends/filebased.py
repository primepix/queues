"""
Backend for queues that implement filebased protocol.

This backend requires either the memcache or cmemcache libraries to be installed.
"""

from queues.backends.base import BaseQueue
from queues import InvalidBackend, QueueException
import os, re
from uuid import uuid1
#try:
#import cPickle as pickle
#except:
import pickle

try:
    from django.conf import settings
    SPOOL = getattr(settings, 'QUEUE_FILEBASED_SPOOL', None)
except:
    SPOOL = os.environ.get('QUEUE_FILEBASED_SPOOL', None)

if not SPOOL:
    raise InvalidBackend("QUEUE_FILEBASED_SPOOL not set.")

class FileBasedSpool():
    def __init__(self, spool):
        self._spool = spool

    def set(self, key, value):
        f = open(os.path.join(self._spool, key), 'wb')
        f.write(pickle.dumps(value))
        f.close()

    def get(self, key):
        f = open(os.path.join(self._spool, key), 'rb')
        result = pickle.loads(f.read())
        f.close()
        return result

    def delete(self, key):
        os.remove(os.path.join(self._spool, key))

class Queue(BaseQueue):
    def __init__(self, name):
        self._connection = FileBasedSpool(SPOOL)
        self.backend = 'filebased'
        self.name = name
        self._connection.set('%s_head' % self.name, None)
        self._connection.set('%s_len' % self.name, 0)

    def read(self):
        try:
            _head = self._connection.get('%s_head' % self.name)
            if _head:
                value, _next = self._connection.get('%s_%s' % (self.name, _head))
                length = self._connection.get('%s_len' % self.name)
                self._connection.set('%s_len' % self.name, length - 1)
                self._connection.delete('%s_%s' % (self.name, _head))
                self._connection.set('%s_head' % self.name, _next)
                return value
            else:
                return None
        except Exception:
            pass

    def write(self, message):
        try:
            _head = self._connection.get('%s_head' % self.name)
            _label = str(uuid1())
            _node_value = (message, _head)
            self._connection.set('%s_%s' % (self.name, _label), _node_value)
            length = self._connection.get('%s_len' % self.name)
            self._connection.set('%s_len' % self.name, length + 1)
            self._connection.set('%s_head' % self.name, _label)
            return True
        except Exception:
            pass

    def __len__(self):
        try:
            try:
                return self._connection.get('%s_len' % self.name)
            except Exception:
                pass
        except AttributeError:
            # If this memcached backend doesn't support starling-style stats
            # or if this queue doesn't exist
            return 0

    def __repr__(self):
        return "<Queue %s>" % self.name

def create_queue():
    """This isn't required, so we noop.  Kept here for swapability."""
    return True

def delete_queue(name):
    """Memcached backends don't provide a way to do this."""
    raise NotImplementedError

def get_list():
    pass
    #"""Supports starling/peafowl-style queue_<name>_items introspection via stats."""
    #conn = Client(CONN.split(';'))
    #queue_list = []
    #queue_re = re.compile(r'queue\_(.*?)\_total_items')
    #try:
        #for server in conn.get_stats():
            ##for key in server[1].keys():
                ##if queue_re.findall(key):
                    ##queue_list.append(queue_re.findall(key)[0])
    #except (KeyError, AttributeError, memcache.MemcachedKeyError, MemcachedStringEncodingError):
        #pass
    #return queue_list
