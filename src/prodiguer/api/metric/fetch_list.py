# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.api.metric.list.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric list group request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.api import utils_handler
from prodiguer.api.metric import utils
from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt



class FetchListRequestHandler(tornado.web.RequestHandler):
    """Simulation list metric request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers."""
        utils.set_cors_white_list(self)


    def _fetch_data(self):
        """Fetches data from db."""
        self.groups = dao.fetch_list()


    def _write_response(self, error=None):
        """Write response output."""
        if not error:
            self.output = {
                'groups': self.groups
            }
        utils_handler.write_response(self, error)


    def _log(self, error=None):
        """Logs request processing completion."""
        utils_handler.log("metric", self, error)


    def get(self):
        # Define tasks.
        tasks = {
            "green": (
                self._fetch_data,
                self._write_response,
                self._log,
                ),
            "red": (
                self._write_response,
                self._log,
                )
        }

        # Invoke tasks.
        rt.invoke(tasks)
