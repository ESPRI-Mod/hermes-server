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



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'


class FetchSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch setup method request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers."""
        utils.set_cors_white_list(self)


    def _validate_request(self):
        """Validates request."""
        if self.request.body:
            utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _decode_request(self):
        """Decodes request."""
        self.group = self.get_argument(_PARAM_GROUP)
        if self.request.body:
            self.query = utils.decode_json_payload(self, False)
        else:
            self.query = None


    def _fetch_data(self):
        """Fetches data from db."""
        self.columns = dao.fetch_columns(self.group, False)
        self.data = dao.fetch_setup(self.group, self.query)


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


    def _process(self):
        """Process one of the support HTTP actions."""
        # Define tasks.
        tasks = {
            "green": (
                self._validate_request,
                self._decode_request,
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


    def get(self):
        self._process()


    def post(self):
        self._process()
