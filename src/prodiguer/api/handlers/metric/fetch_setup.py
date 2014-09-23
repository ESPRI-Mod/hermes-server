# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.api.handlers.metric.fetch_setup.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Metric group setup fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import tornado

from . import utils
from .. import utils as handler_utils
from .... db.mongo import dao_metrics as dao
from .... utils import rt



# Query parameter names.
_PARAM_GROUP = 'group'


class FetchSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch setup method request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers."""
        utils.set_cors_white_list(self)


    def _validate_request_params(self):
        """Validates query params."""
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _decode_request_params(self):
        """Parses query params."""
        self.group = self.get_argument(_PARAM_GROUP)


    def _fetch_data(self):
        """Fetches data from db."""
        self.columns = dao.fetch_columns(self.group, False)
        self.data = dao.fetch_setup(self.group)


    def _write_response(self, error=None):
        """Write response output."""
        if not error:
            self.output = {
                'group': self.group,
                'columns': self.columns,
                'data': self.data
            }
        handler_utils.write(self, error)


    def _log(self, error=None):
        """Logs request processing completion."""
        handler_utils.log("metric", self, error)


    def get(self):
        """HTTP GET request handler.

        """
        # Define tasks.
        tasks = {
            "green": (
                self._validate_request_params,
                self._decode_request_params,
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
