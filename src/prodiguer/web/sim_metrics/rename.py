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
_PARAM_NEW_NAME = 'new_name'


class RenameRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group rename method request handler.

    """
    def _validate_request(self):
        """Validate HTTP POST request.

        """
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))
        utils.validate_group_name(self.get_argument(_PARAM_NEW_NAME),
        						  validate_db_collection=False)


    def _decode_request(self):
        """Decodes request.

        """
        self.group = self.get_argument(_PARAM_GROUP)
        self.new_name = self.get_argument(_PARAM_NEW_NAME)


    def _rename_metric_group(self):
        """Renames metrics group within db.

        """
        dao.rename(self.group, self.new_name)


    def post(self):
    	"""HTTP POST handler.

    	"""
        validation_tasks = [
            self._validate_request
        ]

        processing_tasks = [
            self._decode_request,
            self._rename_metric_group,
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)
