# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.ops.heartbeat.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Operations heartbeat request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime as dt

import tornado

import prodiguer
from prodiguer.web.utils.http1 import process_request




class HeartbeatRequestHandler(tornado.web.RequestHandler):
    """Operations heartbeat request handler.

    """
    def get(self):
        """HTTP GET handler.

        """
        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                "message": "HERMES web service is operational @ {}".format(dt.datetime.now()),
                "version": prodiguer.__version__
            }


        # Process request.
        process_request(self, _set_output)
