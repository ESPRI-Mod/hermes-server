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
from .... utils import rt, convert
from .... db.mongo import dao_metrics as dao



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'
_PARAM_INCLUDE_DB_ID = 'include_db_id'


class FetchRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch method request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers."""
        utils.set_cors_white_list(self)


    def _validate_request(self):
        """Validates request."""
        if self.request.body:
            utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
        utils.validate_format(self)
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))
        utils.validate_include_db_id(self)


    def _decode_request(self):
        """Decodes request."""
        self.group = self.get_argument(_PARAM_GROUP)
        self.include_db_id = utils.decode_include_db_id(self)
        self.format = utils.decode_format(self)
        if self.request.body:
            self.query = utils.decode_json_payload(self, False)
        else:
            self.query = None


    def _fetch_data(self):
        """Fetches data from db.

        """
        self.columns = dao.fetch_columns(self.group, self.include_db_id)
        self.metrics = dao.fetch(self.group, self.include_db_id, self.query)
        self.metrics = [m.values() for m in self.metrics]


    def _set_output(self):
        """Sets response to be returned to client.

        """
        if self.format == 'csv':
            self.output = convert.to_csv(self.columns, self.metrics)
        else:
            self.output = {
                'group': self.group,
                'columns': self.columns,
                'metrics': self.metrics
            }


    def _write_response(self, error=None):
        """Write response output."""
        if error:
            handler_utils.write(self, error)
        elif self.format == 'csv':
            handler_utils.write_csv(self, error)
        else:
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
                self._set_output,
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
