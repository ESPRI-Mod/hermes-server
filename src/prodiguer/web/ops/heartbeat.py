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
            utils_handler.validate_vanilla_request
        ]

        processing_tasks = [
            self._set_output
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)
