# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.web.sim_metrics.list.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric list group request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils



class FetchListRequestHandler(tornado.web.RequestHandler):
    """Simulation list metric request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers.

        """
        utils.set_cors_white_list(self)


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
            'groups': dao.fetch_list()
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
