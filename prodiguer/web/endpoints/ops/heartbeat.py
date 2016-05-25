# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.ops.heartbeat.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Operations heartbeat request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.web.utils.http import HermesHTTPRequestHandler
from prodiguer.web.request_validation import validator as rv



class HeartbeatRequestHandler(HermesHTTPRequestHandler):
    """Operations heartbeat request handler.

    """
    def get(self):
        """HTTP GET handler.

        """
        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                "message": "Hermes web service is running",
                "status": 0
            }

        # Invoke tasks.
        self.invoke(rv.validate, _set_output)
