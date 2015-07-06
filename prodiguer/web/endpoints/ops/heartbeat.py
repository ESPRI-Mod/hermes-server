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

from prodiguer.web.utils import ProdiguerHTTPRequestHandler
from prodiguer.web.utils import request_validation as rv



class HeartbeatRequestHandler(ProdiguerHTTPRequestHandler):
    """Operations heartbeat request handler.

    """
    def get(self):
        """HTTP GET handler.

        """
        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                "message": "Prodiguer web service is running",
                "status": 0
            }

        # Invoke tasks.
        self.invoke(rv.validate, _set_output)
