# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.monitoring.ws.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring websocket handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import tornado.websocket

from .. import utils_ws as ws



# Key used for web socket cache and logging.
_KEY = 'monitoring'


class FrontEndWebSocketHandler(tornado.websocket.WebSocketHandler):
    """Simulation monitoring front end web socket handler.

    """
    def open(self):
        """WS on open event handler."""
        ws.on_connect(_KEY, self)


    def on_close(self):  
        """WS on close event handler."""
        ws.on_disconnect(_KEY, self)
