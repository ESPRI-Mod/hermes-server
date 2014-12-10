# -*- coding: utf-8 -*-

"""
.. module:: utils_ws.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Web socket utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.websocket

from prodiguer.utils import rt, config



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
        rt.log_api("WS {0} :: connection opened :: clients = {1}.".format(key, len(_clients[key])))


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
        rt.log_api("WS {0} :: connection closed :: clients = {1}.".format(key, len(_clients[key])))


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


def pong_clients():
    """Pongs clients.

    """
    logged = False
    for app in _clients.keys():
        for client in _clients[app]:
            if not logged:
                rt.log_api("Ponging websocket clients.")
                logged = True
            try:
                client.write_message("pong")
            except tornado.websocket.WebSocketClosedError:
                _clients[app].remove(client)


def keep_alive():
    """Sends a websocket ping to all clients to keep them open.

    """
    def _do():
        pong_clients()
        keep_alive()

    # Do this every N seconds so as to keep client connections open.
    delay = config.api.websocketKeepAliveDelayInSeconds
    if delay:
        rt.log_api("Websocket keep alive every {0} seconds.".format(delay))
        tornado.ioloop.IOLoop.instance().call_later(delay, _do)
