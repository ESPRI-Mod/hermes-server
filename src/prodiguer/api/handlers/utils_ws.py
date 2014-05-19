# -*- coding: utf-8 -*-

# Module imports.
import tornado.websocket

from ... utils import runtime as rt



# Cached web socket clients.
_clients = {}


def _init_cache(key):
    """Initializes web socket client cache key."""
    if key not in _clients:
        _clients[key] = [];


def get_client_count(key=None):
    """Returns count of connected clients.

    :param key: Web socket client cache key.
    :param type: str

    :returns: Web socket client count.
    :rtype: int

    """        
    if key is not None:
        _init_cache(key)
        return len(_clients[key])
    else:
        return reduce(lambda x, y: x + len(y), _clients.values(), 0)


def on_write(key, data):
    """Broadcasts web socket message to relevant clients.

    :param key: Web socket client cache key.
    :param type: str

    :param data: Data dictionary to send to client.
    :param data: dict

    """    
    _init_cache(key)
    for c in _clients[key]:
        try:
            c.write_message(data)
        # ... remove dead client connections
        except tornado.websocket.WebSocketClosedError:
            _clients[key].remove(c)


def on_connect(key, c):
    """Caches a client connection.

    :param key: Web socket client cache key.
    :param type: str

    :param c: Web socket handler pointer.
    :param c: torndao.websocket.WebSocketHandler

    """     
    _init_cache(key)
    if c not in _clients[key]:
        _clients[key].append(c)
        rt.log_mq("WS {0} :: connection opened :: clients = {1}.".format(key, len(_clients[key])))        
        

def on_disconnect(key, c):
    """Removes a client connection from cache.

    :param key: Web socket client cache key.
    :param type: str

    :param c: Web socket handler pointer.
    :param c: torndao.websocket.WebSocketHandler

    """ 
    _init_cache(key)
    if c in _clients[key]:
        _clients[key].remove(c)
        rt.log_mq("WS {0} :: connection closed :: clients = {1}.".format(key, len(_clients[key])))        


def clear_cache(key=None):
    """Removes client connections from cache.

    :param key: Web socket client cache key.
    :param type: str

    """ 
    global _clients

    if key is None:
        _clients = {}
    elif key in _clients:
        del _clients[key]

