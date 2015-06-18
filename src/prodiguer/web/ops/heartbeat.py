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

from prodiguer.web import utils_handler
from prodiguer.utils import rt



class HeartbeatRequestHandler(tornado.web.RequestHandler):
    """Operations heartbeat request handler.

    """
    def _validate_request(self):
        """Validate HTTP GET request.

        """
        # Invalid if request has associated query, body or files.
        if not utils_handler.is_vanilla_request(self):
            raise tornado.httputil.HTTPInputError()


    def _set_output(self):
        """Sets response to be returned to client.

        """
        self.output = {
            "message": "all good",
            "status": 0
        }


    def get(self):
        """HTTP GET handler.

        """
        validation_tasks = [
            self._validate_request
        ]

        processing_tasks = [
            self._set_output
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)