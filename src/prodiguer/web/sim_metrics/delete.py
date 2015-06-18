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

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'


class DeleteRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group delete method request handler.

    """
    def _validate_request(self):
        """Validate HTTP POST request.

        """
        if self.request.body:
            utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _decode_request(self):
        """Decodes request.

        """
        self.group = self.get_argument(_PARAM_GROUP)
        if self.request.body:
            self.query = utils.decode_json_payload(self, False)
        else:
            self.query = None


    def _delete_metrics(self):
        """Deletes metrics from db.

        """
        dao.delete(self.group, self.query)


    def post(self):
        """HTTP POST handler.

        """
        validation_tasks = [
            self._validate_request
        ]

        processing_tasks = [
            self._decode_request,
            self._delete_metrics,
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)

