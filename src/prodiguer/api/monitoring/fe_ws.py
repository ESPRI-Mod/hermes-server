# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.monitoring.ws.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring websocket handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.websocket

from prodiguer.api.utils import ws



# Key used for web socket cache and logging.
_WS_KEY = 'monitoring'


class FrontEndWebSocketHandler(tornado.websocket.WebSocketHandler):
    """Simulation monitoring front end web socket handler.

    """
    def open(self):
        """WS on open event handler.

        """
        ws.on_connect(_WS_KEY, self)


    def on_close(self):
        """WS on close event handler.

        """
        ws.on_disconnect(_WS_KEY, self)
