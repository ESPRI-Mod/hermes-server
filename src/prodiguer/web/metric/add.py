# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.metric.add.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group add request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from collections import OrderedDict

from prodiguer.web import utils_handler
from prodiguer.web.metric import utils
from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Set of expected payload fields and their type.
_PAYLOAD_FIELDS = set([
    ('group', unicode),
    ('columns', list),
    ('metrics', list),
    ])

# Query parameter names.
_PARAM_DUPLICATE_ACTION = 'duplicate_action'



class AddRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group add method request handler.

    """
    def _validate_request_headers(self):
        """Validates request headers.

        """
        utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)


    def _validate_request_params(self):
        """Validates request query parameters.

        """
        utils.validate_duplicate_action(self.get_argument(_PARAM_DUPLICATE_ACTION))

        # Validation passed therefore decode query params.
        self.duplicate_action = self.get_argument(_PARAM_DUPLICATE_ACTION)


    def _validate_request_payload(self):
        """Validates request payload."""
        # Decode payload.
        payload = utils.decode_json_payload(self)

        # Validate payload.
        utils.validate_payload(payload, _PAYLOAD_FIELDS)

        # Validate group name.
        utils.validate_group_name(payload.group, False)

        # Validate metrics count > 0.
        if len(payload.metrics) == 0:
            raise ValueError("No metrics to add")

        # Validate that length of each metric is same as length of group columns.
        for metric in payload.metrics:
            if len(metric) != len(payload.columns):
                raise ValueError("Invalid metric: number of values does not match number of columns")

        # Validation passed therefore cache decoded payload.
        self.payload = payload


    def _insert_metrics(self):
        """Inserts metrics to the db."""
        def _format(metric):
            """Formats a metric for insertion into db."""
            return [(c, metric[i]) for i, c in enumerate(self.payload.columns)]

        def _format_metrics():
            """Returns list of formatted metrics."""
            return [OrderedDict(_format(m)) for m in self.payload.metrics]

        self.added, self.duplicates = \
            dao.add(self.payload.group, _format_metrics(), self.duplicate_action)


    def _write_response(self, error=None):
        """Write response output."""
        if not error:
            self.output = {
                'group': self.payload.group,
                'added_count': len(self.added),
                'duplicate_count': len(self.duplicates)
            }
        utils_handler.write_response(self, error)


    def _log(self, error=None):
        """Logs request processing completion."""
        utils_handler.log("metric", self, error)


    def post(self):
        # Define tasks.
        tasks = {
            "green": (
                self._validate_request_headers,
                self._validate_request_params,
                self._validate_request_payload,
                self._insert_metrics,
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