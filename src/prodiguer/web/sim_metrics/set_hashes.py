# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.sim_metrics.rename.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group rename request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils



# Query parameter names.
_PARAM_GROUP = 'group'


class SetHashesRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group set hashes method request handler.

    """
    def _validate_request(self):
        """Validate HTTP POST request.

        """
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _decode_request(self):
        """Decodes request.

        """
        self.group = self.get_argument(_PARAM_GROUP)


    def _set_hashes(self):
        """Sets the hash identifiers for all metrics within the group.

        """
        dao.set_hashes(self.group)


    def post(self):
        """HTTP POST handler.

        """
        validation_tasks = [
            self._validate_request
        ]

        processing_tasks = [
            self._decode_request,
            self._set_hashes
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)

