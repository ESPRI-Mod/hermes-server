# -*- coding: utf-8 -*-

"""
.. module:: api.utils_ws.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: API web socket utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import collections

import tornado.websocket

from hermes.utils import config
from hermes.utils import logger
from hermes.utils import data_convertor



# Cached web socket clients.
_WS_CLIENTS = collections.defaultdict(list)


def get_client_count(key=None):
    """Returns count of connected clients.

    :param str key: Web socket client cache key.

    :returns: Web socket client count.
    :rtype: int

    """
    if key is not None:
        return len(_WS_CLIENTS[key])
    else:
        return reduce(lambda x, y: x + len(y), _WS_CLIENTS.values(), 0)


def on_write(key, data, client_filter=None):
    """Broadcasts web socket message to relevant clients.

    :param str key: Web socket client cache key.
    :param dict data: Data dictionary to send to client.
    :param function client_filter: Predicate to determines whether a client is to be written to.

    """
    # Get clients to be broadcast to.
    if client_filter is None:
        clients = _WS_CLIENTS[key]
    else:
        clients = [c for c in _WS_CLIENTS[key] if client_filter(c, data) == True]

    # Escape if there are no clients.
    if not clients:
        return

    # Write data to clients.
    data = data_convertor.jsonify(data)
    for client in clients:
        try:
            client.write_message(data)
        except tornado.websocket.WebSocketClosedError:
            _WS_CLIENTS[key].remove(client)


def on_connect(key, client):
    """Caches a client connection.

    :param str key: Web socket client cache key.
    :param torndao.websocket.WebSocketHandler client: Web socket handler pointer.

    """
    if client not in _WS_CLIENTS[key]:
        _WS_CLIENTS[key].append(client)
        logger.log_web("web-socket connection opended :: clients = {}.".format(len(_WS_CLIENTS[key])))


def on_disconnect(key, client):
    """Removes a client connection from cache.

    :param str key: Web socket client cache key.
    :param torndao.websocket.WebSocketHandler client: Web socket handler pointer.

    """
    if client in _WS_CLIENTS[key]:
        _WS_CLIENTS[key].remove(client)
        logger.log_web("web-socket connection closed :: clients = {}.".format(len(_WS_CLIENTS[key])))


def clear_cache(key=None):
    """Removes client connections from cache.

    :param str key: Web socket client cache key.

    """
    keys = list(_WS_CLIENTS) if key is None else { key }
    for key in keys:
        del _WS_CLIENTS[key]


def pong_clients():
    """Pongs clients.

    """
    for app in _WS_CLIENTS:
        for client in _WS_CLIENTS[app]:
            try:
                client.write_message("pong")
            except tornado.websocket.WebSocketClosedError:
                _WS_CLIENTS[app].remove(client)


def keep_alive():
    """Sends a websocket ping to all clients to keep them open.

    """
    def _do():
        """Performs unit of work."""
        pong_clients()
        keep_alive()

    # Do this every N seconds so as to keep client connections open.
    delay = config.web.websocketKeepAliveDelayInSeconds
    if delay:
        tornado.ioloop.IOLoop.instance().call_later(delay, _do)
