# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.websocket.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring websocket handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.websocket

from prodiguer.web.utils import websockets



# Key used for web socket cache and logging.
_WS_KEY = 'monitoring'


class FrontEndWebSocketAllHandler(tornado.websocket.WebSocketHandler):
    """Simulation monitoring front end web socket handler (all simulations).

    """
    def open(self):
        """WS on open event handler.

        """
        websockets.on_connect(_WS_KEY, self)


    def on_close(self):
        """WS on close event handler.

        """
        websockets.on_disconnect(_WS_KEY, self)
