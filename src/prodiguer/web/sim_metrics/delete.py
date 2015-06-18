# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.sim_metrics.delete.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group delete request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils
from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'


class DeleteRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group delete method request handler.

    """
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


    def _delete_metrics(self):
        """Deletes metrics from db."""
        dao.delete(self.group, self.query)


    def _write_response(self, error=None):
        """Write response output."""
        utils_handler.write_response(self, error)


    def _log(self, error=None):
        """Logs request processing completion."""
        utils_handler.log("metric", self, error)


    def post(self):
        # Define tasks.
        tasks = {
            "green": (
                self._validate_request,
                self._decode_request,
                self._delete_metrics,
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
