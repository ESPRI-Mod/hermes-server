# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.delete.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group delete request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import tornado

from . import utils
from .. import utils as handler_utils
from .... db.mongo import dao_metrics as dao
from ....utils import runtime as rt



# Query parameter names.
_PARAM_GROUP = 'group'


class DeleteRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group delete method request handler.

    """
    def _validate_request_params(self):
        """Validates request params."""
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _decode_request_params(self):
        """Decodes request params."""
        self.group = self.get_argument(_PARAM_GROUP)


    def _delete_metrics(self):
        """Deletes metrics from db."""
        dao.delete(self.group)


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
                self._validate_request_params,
                self._decode_request_params,
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
