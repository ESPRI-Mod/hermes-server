# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.delete.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group delete request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import tornado

from . import utils
from .. import utils as handler_utils
from .... db.mongo import dao_metrics as dao
from ....utils import runtime as rt



# Supported content types.
_CONTENT_TYPE_JSON = "application/json"

# Set of expected payload fields and their type.
_PAYLOAD_FIELDS = set([
    ('group', unicode),
    ('metric_id_list', list)
    ])


class DeleteLinesRequestHandler(tornado.web.RequestHandler):
    """Simulation metric delete lines method request handler.

    """
    def _validate_request_headers(self):
        """Validates request headers."""
        utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)


    def _validate_request_payload(self):
        """Validates request body."""
        # Decode payload.
        payload = utils.decode_json_payload(self)

        # Validate payload.
        utils.validate_payload(payload, _PAYLOAD_FIELDS)

        # Validate group name.
        utils.validate_group_name(payload.group, False)

        # Validation passed therefore cache decoded payload.
        self.payload = payload


    def _delete_data(self):
        """Deletes data from db."""
        dao.delete_lines(self.payload.group, self.payload.metric_id_list)


    def _write_response(self, error=None):
        """Write response output."""
        handler_utils.write(self, error)


    def _log(self, error=None):
        """Logs request processing completion."""
        handler_utils.log("metric", self, error)


    def post(self):
        # Define tasks.
        tasks = {
            "green": (
                self._validate_request_headers,
                self._validate_request_payload,
                self._delete_data,
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
