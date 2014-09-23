# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.api.handlers.metric.fetch.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import tornado

from . import utils
from .. import utils as handler_utils
from .... utils import rt
from .... db.mongo import dao_metrics as dao



# Query parameter names.
_PARAM_GROUP = 'group'
_PARAM_INCLUDE_DB_ID = 'include_db_id'


class FetchRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch method request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers."""
        utils.set_cors_white_list(self)


    def _validate_request_params(self):
        """Validates request params."""
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))
        utils.validate_include_db_id(self)


    def _decode_request_params(self):
        """Decodes request query parameters."""
        self.group = self.get_argument(_PARAM_GROUP)
        self.include_db_id = utils.decode_include_db_id(self)


    def _fetch_data(self):
        """Fetches data from db."""
        self.columns = dao.fetch_columns(self.group, self.include_db_id)
        self.metrics = [m.values() for m in dao.fetch(self.group, self.include_db_id)]


    def _write_response(self, error=None):
        """Write response output."""
        if not error:
            self.output = {
                'group': self.group,
                'columns': self.columns,
                'metrics': self.metrics
            }
        handler_utils.write(self, error)


    def _log(self, error=None):
        """Logs request processing completion."""
        handler_utils.log("metric", self, error)


    def get(self):
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
