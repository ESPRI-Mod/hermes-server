# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.web.sim_metrics.fetch.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils



# Query parameter names.
_PARAM_GROUP = 'group'


class FetchColumnsRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch columns method request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers.

        """
        utils.set_cors_white_list(self)


    def _validate_request(self):
        """Validate HTTP GET request.

        """
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _decode_request(self):
        """Decodes request.

        """
        self.group = self.get_argument(_PARAM_GROUP)


    def _set_output(self):
        """Sets response to be returned to client.

        """
        self.output = {
            'group': self.group,
            'columns': dao.fetch_columns(self.group, True)
        }


    def get(self):
        """HTTP GET handler.

        """
        validation_tasks = [
            self._validate_request
        ]

        processing_tasks = [
            self._decode_request,
            self._set_output,
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)
